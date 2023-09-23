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

const MODBUS_METRICS: [(&str, u16, ModbusType); 33] = [
    ("fems_state", 222, U16),
    ("fems_ess_soc", 302, U16),
    ("fems_ess_active_power", 303, F32),
    ("fems_ess_reactive_power", 309, F32),
    ("fems_grid_active_power", 315, F32),
    ("fems_production_active_power", 327, F32),
    ("fems_production_ac_active_power", 331, F32),
    ("fems_production_dc_actual_power", 339, F32),
    ("fems_consumption_active_power", 343, F32),
    ("fems_ess_active_charge_energy", 351, F64),
    ("fems_ess_active_discharge_energy", 355, F64),
    ("fems_grid_buy_active_energy", 359, F64),
    ("fems_grid_sell_active_energy", 363, F64),
    ("fems_production_active_energy", 367, F64),
    ("fems_production_ac_active_energy", 371, F64),
    ("fems_production_dc_active_energy", 375, F64),
    ("fems_consumption_active_energy", 379, F64),
    ("fems_ess_dc_charge_energy", 383, F64),
    ("fems_ess_dc_discharge_energy", 387, F64),
    ("fems_ess_active_power_l1", 391, F32),
    ("fems_ess_active_power_l2", 393, F32),
    ("fems_ess_active_power_l3", 395, F32),
    ("fems_grid_active_power_l1", 397, F32),
    ("fems_grid_active_power_l2", 399, F32),
    ("fems_grid_active_power_l3", 401, F32),
    ("fems_production_ac_active_power_l1", 403, F32),
    ("fems_production_ac_active_power_l2", 405, F32),
    ("fems_production_ac_active_power_l3", 407, F32),
    ("fems_consumption_ac_active_power_l1", 409, F32),
    ("fems_consumption_ac_active_power_l2", 411, F32),
    ("fems_consumption_ac_active_power_l3", 413, F32),
    ("fems_ess_discharge_power", 415, F32),
    ("fems_grid_mode", 417, U16),
];

#[derive(Deserialize)]
struct Params {
    host: SocketAddr,
    fems_id: String,
}

#[allow(unused_variables)]
async fn metrics(
    Query(Params { host, fems_id: name }): Query<Params>,
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

    for (metric_name, address, modbus_type) in MODBUS_METRICS {
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

        report.push_str(&format!("{metric_name}{{fems_id = \"{name}\"}} {value}\n"));
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

