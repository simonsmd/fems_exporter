use std::{
    collections::{hash_map::Entry, HashMap},
    net::{SocketAddr, IpAddr, Ipv4Addr},
    sync::Arc, error::Error,
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Router,
};
use clap::Parser;
use tokio::{sync::Mutex, signal};
use tokio_modbus::{client::Context, prelude::*};

use serde::Deserialize;

enum ModbusType {
    U16,
    F32,
    F64,
}

use ModbusType::{U16, F32, F64};

impl ModbusType {
    fn register_count(&self) -> u16 {
        match self {
            U16 => 1,
            F32 => 2,
            F64 => 4,
        }
    }
}

fn decode_u16(data: &[u16]) -> u16 {
    *data.first().unwrap()
}

fn decode_f32(data: &[u16]) -> f32 {
    let bytes: [u8; 4] = data
        .iter()
        .flat_map(|word| word.to_be_bytes())
        .collect::<Vec<u8>>()
        .try_into()
        .unwrap();
    f32::from_be_bytes(bytes)
}

fn decode_f64(data: &[u16]) -> f64 {
    let bytes: [u8; 8] = data
        .iter()
        .flat_map(|word| word.to_be_bytes())
        .collect::<Vec<u8>>()
        .try_into()
        .unwrap();
    f64::from_be_bytes(bytes)
}

const MODBUS_METRICS: [(&str, &[(&str, &str)], u16, ModbusType); 32] = [
    ("fems_state", &[], 222, U16),
    ("fems_grid_mode", &[], 417, U16),
    ("fems_ess_soc_percent", &[], 302, U16),
    ("fems_ess_power_watts_total", &[], 303, F32),
    ("fems_ess_power_watts", &[("phase", "l1")], 391, F32),
    ("fems_ess_power_watts", &[("phase", "l2")], 393, F32),
    ("fems_ess_power_watts", &[("phase", "l3")], 395, F32),
    ("fems_ess_discharge_power_watts_total", &[], 415, F32),
    ("fems_ess_reactive_power_voltampere", &[], 309, F32),
    ("fems_grid_power_watts_total", &[], 315, F32),
    ("fems_grid_power_watts", &[("phase", "l1")], 397, F32),
    ("fems_grid_power_watts", &[("phase", "l2")], 399, F32),
    ("fems_grid_power_watts", &[("phase", "l3")], 401, F32),
    ("fems_production_power_watts_total", &[], 327, F32),
    ("fems_production_power_watts", &[("type", "dc")], 339, F32),
    ("fems_production_power_watts", &[("type", "ac"), ("phase", "l1")], 403, F32),
    ("fems_production_power_watts", &[("type", "ac"), ("phase", "l2")], 405, F32),
    ("fems_production_power_watts", &[("type", "ac"), ("phase", "l3")], 407, F32),
    ("fems_consumption_power_watts_total", &[], 343, F32),
    ("fems_consumption_power_watts", &[("phase", "l3")], 409, F32),
    ("fems_consumption_power_watts", &[("phase", "l3")], 411, F32),
    ("fems_consumption_power_watts", &[("phase", "l3")], 413, F32),
    ("fems_ess_charge_energy_watthours", &[], 351, F64),
    ("fems_ess_discharge_energy_watthours", &[], 355, F64),
    ("fems_ess_dc_charge_energy_watthours", &[], 383, F64),
    ("fems_ess_dc_discharge_energy_watthours", &[], 387, F64),
    ("fems_grid_buy_energy_watthours", &[], 359, F64),
    ("fems_grid_sell_energy_watthours", &[], 363, F64),
    ("fems_production_energy_watthours_total", &[], 367, F64),
    ("fems_production_energy_watthours", &[("type", "ac")], 371, F64),
    ("fems_production_energy_watthours", &[("type", "dc")], 375, F64),
    ("fems_consumption_energy_watthours", &[], 379, F64),
];

#[derive(Deserialize)]
struct Params {
    host: SocketAddr,
    fems_id: String,
}

#[allow(unused_variables)]
async fn metrics(
    Query(Params { host, fems_id }): Query<Params>,
    State(state): State<ModbusState>,
) -> (StatusCode, String) {
    // Get existing connection or open a new one
    let mut contexts = state.0.lock().await;
    let ctx = match contexts.entry(host) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => {
            let mut ctx = match tcp::connect(host).await {
                Ok(ctx) => ctx,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("unable to connect to fems modbus at {host}: {e}"),
                    )
                }
            };

            ctx.set_slave(Slave(1));
            e.insert(ctx)
        }
    };

    let mut report = String::new();

    for (metric_name, labels, address, modbus_type) in MODBUS_METRICS {
        let data = ctx
            .read_input_registers(address, modbus_type.register_count())
            .await;

        let data = match data {
            Ok(data) => data,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unable to read modbus input register: {e}"),
                )
            }
        };

        let value = match modbus_type {
            U16 => decode_u16(&data).to_string(),
            F32 => decode_f32(&data).to_string(),
            F64 => decode_f64(&data).to_string(),
        };

        let mut labels: Vec<(&str, &str)> = labels.into();
        labels.push(("fems_id", &fems_id));

        let labels: Vec<String> = labels.iter().map(|(l, v)| format!("{l} = \"{v}\"")).collect();
        let labels = labels.join(", ");

        report.push_str(&format!("{metric_name}{{{labels}}} {value}\n"));
    }

    (StatusCode::OK, report)
}

#[derive(Clone)]
struct ModbusState(Arc<Mutex<HashMap<SocketAddr, Context>>>);

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 80)]
    port: u16,
    #[arg(short, long, default_value_t = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))]
    bind: IpAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let bind_address = SocketAddr::new(args.bind, args.port);

    let app = Router::new()
        .route("/metrics", get(metrics))
        .with_state(ModbusState(Arc::new(Mutex::new(HashMap::new()))));

    axum::Server::bind(&bind_address)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    println!("signal received, starting graceful shutdown");
}

