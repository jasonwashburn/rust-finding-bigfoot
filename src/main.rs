use serde::{Deserialize, Serialize};
use std::{error::Error, process};

const BIGFOOT_DATA_FILE_PATH: &str = "data/bfro_reports_geocoded.csv";

#[derive(Debug, Deserialize, Serialize)]
struct Sighting {
    observed: Option<String>,
    location_details: Option<String>,
    county: String,
    state: String,
    title: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    date: Option<String>,
    number: f64,
    classification: String,
    geohash: Option<String>,
    temperature_high: Option<f64>,
    temperature_mid: Option<f64>,
    temperature_low: Option<f64>,
    dew_point: Option<f64>,
    humidity: Option<f64>,
    cloud_cover: Option<f64>,
    moon_phase: Option<f64>,
    precip_intensity: Option<f64>,
    precip_probability: Option<f64>,
    precip_type: Option<String>,
    pressure: Option<f64>,
    summary: Option<String>,
    uv_index: Option<f64>,
    visibility: Option<f64>,
    wind_bearing: Option<f64>,
    wind_speed: Option<f64>,
}

fn main() {
    let sightings = match load_data() {
        Ok(sightings) => sightings,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    if let Err(err) = write_to_redis(sightings) {
        println!("{}", err);
        process::exit(1);
    }
}

fn load_data() -> Result<Vec<Sighting>, Box<dyn Error>> {
    let mut results: Vec<Sighting> = Vec::new();
    let mut reader = csv::Reader::from_path(BIGFOOT_DATA_FILE_PATH)?;
    for result in reader.deserialize() {
        let record: Sighting = result?;
        results.push(record)
    }
    Ok(results)
}

fn write_to_redis(sightings: Vec<Sighting>) -> redis::RedisResult<()> {
    let start = std::time::Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut con = client.get_connection()?;
    for record in sightings.iter() {
        let idx = record.number;
        let json = serde_json::to_string(&record).unwrap();
        redis::cmd("JSON.SET")
            .arg(format!("sighting:{idx}"))
            .arg("$")
            .arg(&json)
            .query(&mut con)?;
    }
    let num_records = sightings.len();
    let duration = start.elapsed();
    println!("Loaded {num_records} records in {:?} seconds", duration);
    Ok(())
}
