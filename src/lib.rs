pub use freetds;
use freetds::connection::ConnectionBuilder;
pub use freetds::{
    Connection,
    ResultSet,
    ColumnId,
    Error,
    error::Type,
    to_sql::ToSql,
    Statement,
    Value,
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
};
pub use r2d2;
use r2d2::ManageConnection;

#[derive(Debug,Clone)]
pub struct FreetdsConnectionManager {
    builder: ConnectionBuilder,
}

impl FreetdsConnectionManager {
    pub fn new(builder: ConnectionBuilder) -> Self {
        Self { builder }
    }
}

impl ManageConnection for FreetdsConnectionManager {
    type Connection = freetds::Connection;
    type Error = freetds::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(self.builder.connect()?)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let db_name = conn.db_name()?;

        let want_database = self.builder.get_database().unwrap_or("master".to_string());
        if db_name != want_database {
            conn.execute(&format!("use {}", want_database), &[])?;
        }
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.is_connected()
    }

}

#[cfg(test)]
mod tests {
    use std::{time::Duration, thread};
    use crate::FreetdsConnectionManager;

    const SERVER: &str = "192.168.130.221:2025";

    #[test]
    fn test_freetds_connection_manager() {
        let builder = freetds::Connection::builder()
            .server_name(SERVER)
            .username("sa")
            .password("");
        let manager = FreetdsConnectionManager::new(builder);
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .max_lifetime(Some(Duration::from_secs(5)))
            .build(manager)
            .unwrap();

        let mut handles = Vec::new();
        for i in 0..15 {
            let pool = pool.clone();
            let handle = thread::spawn(move || {
                let mut conn = pool.get().unwrap();
                let mut rs = conn.execute("select getdate()", &[]).unwrap();
                while rs.next() {
                    println!("[{}] {}", i, rs.get_string(0).unwrap().unwrap());
                }
                thread::sleep(Duration::from_millis(1000));
            });
            handles.push(handle);
        }

        while let Some(handle) = handles.pop() {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_is_valid() {
        /* The connection should restore current database */
        let builder = freetds::Connection::builder()
            .server_name(SERVER)
            .username("sa")
            .password("");
        let manager = FreetdsConnectionManager::new(builder);
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .max_lifetime(Some(Duration::from_secs(5)))
            .build(manager)
            .unwrap();
        
        let mut conn = pool.get().unwrap();
        conn.execute("use sybsystemprocs", &[]).unwrap();
        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("sybsystemprocs")), rs.get_string(0).unwrap());
        drop(conn);

        let mut conn = pool.get().unwrap();
        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("master")), rs.get_string(0).unwrap());
    }

}

