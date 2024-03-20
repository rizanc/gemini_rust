use openssl::ssl::SslConnector;
use openssl::ssl::SslMethod;
use openssl::ssl::SslVerifyMode;
use postgres_openssl::MakeTlsConnector;
use std::env;
use log::trace;

pub struct OneMinuteTable {
    pub ts: f64,
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub vol: f64,
}

pub struct CratesDB {
    pub client: postgres::Client,
}

impl CratesDB {
    pub fn new() -> Self {
        trace!("New CratesDB");
        CratesDB {
            client: CratesDB::connect().unwrap(),
        }
    }

    fn connect() -> Result<postgres::Client, tokio_postgres::error::Error> {
        trace!("Connect CratesDB");
        let api_cert: String;
        match env::var("api_cert") {
            Ok(val) => {
                api_cert = val;
            }
            Err(_) => panic!("api_cert not found"),
        }

        let db_connection: String;
        match env::var("db_connection") {
            Ok(val) => {
                db_connection = val;
            }
            Err(_) => panic!("api_cert not found"),
        }

        let mut builder =
            SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
        builder
            .set_ca_file(api_cert)
            .expect("unable to load ca.cert");
        builder.set_verify(SslVerifyMode::NONE);
        let connector = MakeTlsConnector::new(builder.build());

        return postgres::Client::connect(&db_connection, connector);
    }

    pub fn upsert(&mut self, record: OneMinuteTable) -> Result<u64, tokio_postgres::Error> {
        let query = "
            INSERT INTO crate.one_minute_2 (ts, symbol, open, high, low, close, vol)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (ts, symbol)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                vol = EXCLUDED.vol";

        self.client.execute(
            query,
            &[
                &record.ts,
                &record.symbol,
                &record.open,
                &record.high,
                &record.low,
                &record.close,
                &record.vol,
            ],
        )
    }


    pub fn init_db(&mut self) -> Result<(), postgres::Error> {
        return self.client.batch_execute(
            "
        drop table one_minute_2; 
        
        CREATE TABLE one_minute_2 (
            ts double precision NOT NULL,
            symbol text NOT NULL,
            open double precision,
            high double precision,
            low double precision,
            close double precision,
            vol double precision,
            PRIMARY KEY (ts, symbol)
        )
    ",
        );
    }
}
