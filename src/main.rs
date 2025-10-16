// // ============================================================================
// // FILE: src/main.rs
// // AI DISEASE TRAINING DATA SYNC - COMPLETE PROJECT
// // Direct SQL INSERT with Complex JOIN Query
// // UPDATED: Complete and Ready to Run
// // ============================================================================

// use sqlx::{MySqlPool, Row, mysql::MySqlPoolOptions};
// use log::{info, warn, error, debug};
// use chrono::Local;
// use std::time::Duration;
// use std::sync::{Arc, Mutex};
// use flexi_logger::{Logger, FileSpec, WriteMode, Criterion};
// use num_cpus;
// use std::fs;

// // ============================================================================
// // L1: DATA STRUCTURES & CONSTANTS
// // ============================================================================

// #[derive(Debug, Clone)]
// struct SyncConfig {
//     db_src: String,
//     db_dst: String,
//     src_db: String,
//     dst_db: String,
//     batch_size: usize,
//     limit: usize,
//     max_workers: usize,
// }

// #[derive(Debug, Clone)]
// struct SyncStats {
//     total_processed: usize,
//     total_inserted: usize,
//     total_errors: usize,
//     total_duration: Duration,
//     execution_time: f64,
// }

// #[derive(Debug)]
// struct PerformanceMonitor {
//     start_time: std::time::Instant,
//     checkpoints: Arc<Mutex<Vec<(String, std::time::Instant)>>>,
// }

// #[derive(Debug)]
// enum SyncMode {
//     Full,
//     Incremental(i32),
//     HealthCheck,
//     Preview,
//     Verify,
// }

// // ============================================================================
// // L2.1: LOGGER SUBSYSTEM - ENHANCED
// // ============================================================================

// mod logger_system {
//     use super::*;

//     pub fn init_logger() -> Result<(), Box<dyn std::error::Error>> {
//         fs::create_dir_all("logs")?;

//         Logger::try_with_str("debug")?
//             .log_to_file(
//                 FileSpec::default()
//                     .directory("logs")
//                     .basename("disease_sync"),
//             )
//             .log_to_stderr()
//             .duplicate_to_stderr(flexi_logger::Duplicate::All)
//             .write_mode(WriteMode::BufferAndFlush)
//             .rotate(
//                 Criterion::Size(10_485_760),
//                 flexi_logger::Naming::Numbers,
//                 flexi_logger::Cleanup::KeepLogFiles(5),
//             )
//             .format(|w, now, record| {
//                 write!(
//                     w,
//                     "[{}] [{}] {}",
//                     now.format("%Y-%m-%d %H:%M:%S%.3f"),
//                     record.level(),
//                     &record.args()
//                 )
//             })
//             .start()?;

//         info!("════════════════════════════════════════════════");
//         info!("✅ Logger initialized - File: logs/disease_sync_*.log");
//         info!("════════════════════════════════════════════════");
        
//         Ok(())
//     }
// }

// // ============================================================================
// // L2.2: CONNECTION MANAGER SUBSYSTEM
// // ============================================================================

// mod connection_manager {
//     use super::*;

//     pub async fn create_pool(
//         connection_string: &str,
//         max_connections: u32,
//         pool_name: &str,
//     ) -> Result<MySqlPool, Box<dyn std::error::Error>> {
//         debug!("📡 Creating connection pool '{}' with max_connections={}", pool_name, max_connections);

//         let pool = MySqlPoolOptions::new()
//             .max_connections(max_connections)
//             .acquire_timeout(Duration::from_secs(30))
//             .idle_timeout(Duration::from_secs(300))
//             .max_lifetime(Duration::from_secs(1800))
//             .connect(connection_string)
//             .await?;

//         info!("✅ Connection pool '{}' created successfully", pool_name);
//         debug!("   Connection string: {}", connection_string.replace(&get_password_from_string(connection_string), "***"));
//         Ok(pool)
//     }

//     pub async fn verify_connection(
//         pool: &MySqlPool,
//         db_name: &str,
//     ) -> Result<bool, Box<dyn std::error::Error>> {
//         debug!("🔍 Verifying connection to database: {}", db_name);

//         match sqlx::query_as::<_, (i32,)>("SELECT 1").fetch_one(pool).await {
//             Ok((result,)) => {
//                 info!("✅ Database connection verified for: {}", db_name);
//                 Ok(result == 1)
//             }
//             Err(e) => {
//                 error!("❌ Connection verification failed for {}: {}", db_name, e);
//                 Err(Box::new(e))
//             }
//         }
//     }

//     pub async fn get_source_record_count(
//         pool: &MySqlPool,
//         src_db: &str,
//     ) -> Result<i64, Box<dyn std::error::Error>> {
//         let sql = format!("SELECT COUNT(*) FROM `{}`.opdscreen", src_db);
//         let result: (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
//         debug!("Source record count from {}.opdscreen: {}", src_db, result.0);
//         Ok(result.0)
//     }

//     fn get_password_from_string(conn_str: &str) -> String {
//         conn_str
//             .split(':')
//             .nth(1)
//             .and_then(|s| s.split('@').next())
//             .unwrap_or("****")
//             .to_string()
//     }
// }

// // ============================================================================
// // L2.3: TABLE MANAGEMENT SUBSYSTEM
// // ============================================================================

// mod table_manager {
//     use super::*;

//     pub async fn create_training_table(
//         pool: &MySqlPool,
//         db_name: &str,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         debug!("📋 Creating training data table in {}...", db_name);

//         let create_table_sql = format!(
//             r#"
//             CREATE TABLE IF NOT EXISTS `{db_name}`.`ai_disease_training_data` (
//                 `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
//                 `visit_id` VARCHAR(50) UNIQUE NOT NULL,
//                 `hn` VARCHAR(9),
//                 `vn` VARCHAR(13),
//                 `symptoms` LONGTEXT,
//                 `icd10_code` VARCHAR(9),
//                 `disease_name` VARCHAR(255),
//                 `medicines` LONGTEXT,
//                 `age` INT,
//                 `sex` CHAR(1),
//                 `visit_date` DATE,
//                 `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//                 `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
//                 INDEX `idx_hn` (`hn`),
//                 INDEX `idx_vn` (`vn`),
//                 INDEX `idx_icd10` (`icd10_code`),
//                 INDEX `idx_visit_date` (`visit_date`),
//                 INDEX `idx_age` (`age`)
//             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
//             "#,
//             db_name = db_name
//         );

//         info!("📋 Creating/Verifying table: {}.ai_disease_training_data", db_name);
//         match sqlx::query(&create_table_sql).execute(pool).await {
//             Ok(_) => {
//                 info!("✅ Table created/verified successfully");
//                 Ok(())
//             }
//             Err(e) => {
//                 error!("❌ Failed to create table: {}", e);
//                 Err(Box::new(e))
//             }
//         }
//     }

//     pub async fn clear_table(
//         pool: &MySqlPool,
//         db_name: &str,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         info!("🗑️ Clearing existing training data table...");
//         let truncate_sql = format!("TRUNCATE TABLE `{}`.`ai_disease_training_data`", db_name);
        
//         match sqlx::query(&truncate_sql).execute(pool).await {
//             Ok(result) => {
//                 info!("✅ Table cleared - Rows affected: {}", result.rows_affected());
//                 Ok(())
//             }
//             Err(e) => {
//                 error!("❌ Failed to clear table: {}", e);
//                 Err(Box::new(e))
//             }
//         }
//     }

//     pub async fn get_table_count(
//         pool: &MySqlPool,
//         db_name: &str,
//     ) -> Result<i64, Box<dyn std::error::Error>> {
//         let sql = format!("SELECT COUNT(*) as cnt FROM `{}`.`ai_disease_training_data`", db_name);
//         let result: (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
//         debug!("Current table count: {}", result.0);
//         Ok(result.0)
//     }
// }

// // ============================================================================
// // L2.4: DIRECT SQL EXECUTION SUBSYSTEM (Main Logic)
// // ============================================================================

// mod sql_executor {
//     use super::*;

//     fn get_insert_query(src_db: &str, dst_db: &str) -> String {
//         format!(
//             r#"
//             INSERT INTO `{dst_db}`.`ai_disease_training_data` 
//             (visit_id, hn, vn, symptoms, icd10_code, disease_name, medicines, age, sex, visit_date)
//             SELECT 
//                 CONCAT(o.hn, '-', o.vn) as visit_id,
//                 o.hn,
//                 o.vn,
//                 COALESCE(o.cc, 'Unknown') as symptoms,
//                 COALESCE(i.code, 'Unknown') as icd10_code,
//                 COALESCE(i.name, 'Unknown') as disease_name,
//                 COALESCE(GROUP_CONCAT(DISTINCT CONCAT(d.name, ' ', COALESCE(d.strength, '')) SEPARATOR '|'), 'Unknown') as medicines,
//                 YEAR(CURDATE()) - YEAR(COALESCE(o.vstdate, CURDATE())) as age,
//                 COALESCE(h.sex, 'U') as sex,
//                 o.vstdate as visit_date
//             FROM `{src_db}`.opdscreen o
//             LEFT JOIN `{src_db}`.vn_stat v ON v.vn = o.vn
//             LEFT JOIN `{src_db}`.icd101 i ON i.code = v.pdx
//             LEFT JOIN `{src_db}`.opitemrece op ON op.vn = o.vn
//             LEFT JOIN `{src_db}`.drugitems d ON d.icode = op.icode
//             LEFT JOIN `{src_db}`.hismember h ON h.hn = o.hn
//             WHERE i.code IS NOT NULL 
//               AND TRIM(COALESCE(v.pdx, '')) != ''
//             GROUP BY o.hn, o.vn, i.code, o.vstdate
//             ORDER BY o.vstdate DESC
//             LIMIT ?
//             "#,
//             src_db = src_db,
//             dst_db = dst_db
//         )
//     }

//     pub async fn execute_full_sync(
//         src_pool: &MySqlPool,
//         dst_pool: &MySqlPool,
//         config: &SyncConfig,
//     ) -> Result<SyncStats, Box<dyn std::error::Error>> {
//         info!("🚀 Starting FULL SYNC with direct SQL INSERT...");
//         info!("════════════════════════════════════════════════");
//         let start_time = std::time::Instant::now();

//         let insert_sql = get_insert_query(&config.src_db, &config.dst_db);

//         info!("📊 Building complex JOIN query...");
//         info!("🔗 Tables involved: opdscreen, vn_stat, icd101, opitemrece, drugitems, hismember");
//         info!("📦 Processing up to {} records", config.limit);
//         debug!("Generated SQL: {}", insert_sql.lines().take(5).collect::<Vec<_>>().join(" "));

//         let source_count = connection_manager::get_source_record_count(src_pool, &config.src_db).await?;
//         info!("✅ Source opdscreen has {} records", source_count);

//         if source_count == 0 {
//             warn!("⚠️ No source data found");
//             return Ok(SyncStats {
//                 total_processed: 0,
//                 total_inserted: 0,
//                 total_errors: 0,
//                 total_duration: start_time.elapsed(),
//                 execution_time: 0.0,
//             });
//         }

//         info!("💾 Executing INSERT INTO...SELECT with JOINs...");
//         match sqlx::query(&insert_sql)
//             .bind(config.limit as u32)
//             .execute(dst_pool)
//             .await
//         {
//             Ok(result) => {
//                 let rows_affected = result.rows_affected() as usize;
//                 info!("✅ Query executed successfully");
//                 info!("📈 Rows affected: {}", rows_affected);

//                 let final_count = table_manager::get_table_count(dst_pool, &config.dst_db).await?;
//                 info!("✅ Final record count in destination: {}", final_count);
//                 info!("════════════════════════════════════════════════");

//                 let duration = start_time.elapsed();
//                 let execution_time = duration.as_secs_f64();

//                 Ok(SyncStats {
//                     total_processed: rows_affected,
//                     total_inserted: rows_affected,
//                     total_errors: 0,
//                     total_duration: duration,
//                     execution_time,
//                 })
//             }
//             Err(e) => {
//                 error!("❌ Query execution failed: {}", e);
//                 error!("SQL (first 500 chars): {}", &insert_sql[..insert_sql.len().min(500)]);
//                 Err(Box::new(e))
//             }
//         }
//     }

//     pub async fn execute_incremental_sync(
//         src_pool: &MySqlPool,
//         dst_pool: &MySqlPool,
//         config: &SyncConfig,
//         hours: i32,
//     ) -> Result<SyncStats, Box<dyn std::error::Error>> {
//         info!("🔄 Starting INCREMENTAL SYNC (last {} hours)...", hours);
//         info!("════════════════════════════════════════════════");
//         let start_time = std::time::Instant::now();

//         let incremental_sql = format!(
//             r#"
//             INSERT INTO `{dst_db}`.`ai_disease_training_data` 
//             (visit_id, hn, vn, symptoms, icd10_code, disease_name, medicines, age, sex, visit_date)
//             SELECT 
//                 CONCAT(o.hn, '-', o.vn) as visit_id,
//                 o.hn,
//                 o.vn,
//                 COALESCE(o.cc, 'Unknown') as symptoms,
//                 COALESCE(i.code, 'Unknown') as icd10_code,
//                 COALESCE(i.name, 'Unknown') as disease_name,
//                 COALESCE(GROUP_CONCAT(DISTINCT CONCAT(d.name, ' ', COALESCE(d.strength, '')) SEPARATOR '|'), 'Unknown') as medicines,
//                 YEAR(CURDATE()) - YEAR(COALESCE(o.vstdate, CURDATE())) as age,
//                 COALESCE(h.sex, 'U') as sex,
//                 o.vstdate as visit_date
//             FROM `{src_db}`.opdscreen o
//             LEFT JOIN `{src_db}`.vn_stat v ON v.vn = o.vn
//             LEFT JOIN `{src_db}`.icd101 i ON i.code = v.pdx
//             LEFT JOIN `{src_db}`.opitemrece op ON op.vn = o.vn
//             LEFT JOIN `{src_db}`.drugitems d ON d.icode = op.icode
//             LEFT JOIN `{src_db}`.hismember h ON h.hn = o.hn
//             WHERE i.code IS NOT NULL 
//               AND TRIM(COALESCE(v.pdx, '')) != ''
//               AND o.vstdate >= DATE_SUB(NOW(), INTERVAL ? HOUR)
//             GROUP BY o.hn, o.vn, i.code, o.vstdate
//             ON DUPLICATE KEY UPDATE
//                 symptoms = VALUES(symptoms),
//                 disease_name = VALUES(disease_name),
//                 medicines = VALUES(medicines),
//                 age = VALUES(age)
//             "#,
//             src_db = config.src_db,
//             dst_db = config.dst_db
//         );

//         info!("⏰ Syncing data from last {} hours", hours);
//         info!("🔍 Checking for recent updates...");

//         match sqlx::query(&incremental_sql)
//             .bind(hours)
//             .execute(dst_pool)
//             .await
//         {
//             Ok(result) => {
//                 let rows_affected = result.rows_affected() as usize;
//                 info!("✅ Incremental sync completed");
//                 info!("📈 Rows affected: {}", rows_affected);
//                 info!("════════════════════════════════════════════════");

//                 let duration = start_time.elapsed();

//                 Ok(SyncStats {
//                     total_processed: rows_affected,
//                     total_inserted: rows_affected,
//                     total_errors: 0,
//                     total_duration: duration,
//                     execution_time: duration.as_secs_f64(),
//                 })
//             }
//             Err(e) => {
//                 error!("❌ Incremental sync failed: {}", e);
//                 Err(Box::new(e))
//             }
//         }
//     }

//     pub async fn preview_data(
//         src_pool: &MySqlPool,
//         config: &SyncConfig,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         info!("👁️ Previewing first 10 records from source query...");
//         info!("════════════════════════════════════════════════");

//         let preview_sql = format!(
//             r#"
//             SELECT 
//                 CONCAT(o.hn, '-', o.vn) as visit_id,
//                 o.hn,
//                 o.vn,
//                 COALESCE(o.cc, 'Unknown') as symptoms,
//                 COALESCE(i.code, 'Unknown') as icd10_code,
//                 COALESCE(i.name, 'Unknown') as disease_name,
//                 COALESCE(GROUP_CONCAT(DISTINCT CONCAT(d.name, ' ', COALESCE(d.strength, '')) SEPARATOR '|'), 'Unknown') as medicines,
//                 YEAR(CURDATE()) - YEAR(COALESCE(o.vstdate, CURDATE())) as age,
//                 COALESCE(h.sex, 'U') as sex,
//                 o.vstdate as visit_date
//             FROM `{src_db}`.opdscreen o
//             LEFT JOIN `{src_db}`.vn_stat v ON v.vn = o.vn
//             LEFT JOIN `{src_db}`.icd101 i ON i.code = v.pdx
//             LEFT JOIN `{src_db}`.opitemrece op ON op.vn = o.vn
//             LEFT JOIN `{src_db}`.drugitems d ON d.icode = op.icode
//             LEFT JOIN `{src_db}`.hismember h ON h.hn = o.hn
//             WHERE i.code IS NOT NULL 
//               AND TRIM(COALESCE(v.pdx, '')) != ''
//             GROUP BY o.hn, o.vn, i.code, o.vstdate
//             ORDER BY o.vstdate DESC
//             LIMIT 10
//             "#,
//             src_db = config.src_db
//         );

//         let rows = sqlx::query(&preview_sql).fetch_all(src_pool).await?;

//         info!("📊 Preview: {} records found", rows.len());
//         info!("");

//         for (idx, row) in rows.iter().enumerate() {
//             let visit_id: String = row.try_get("visit_id").unwrap_or_default();
//             let hn: String = row.try_get("hn").unwrap_or_default();
//             let vn: String = row.try_get("vn").unwrap_or_default();
//             let disease: String = row.try_get("disease_name").unwrap_or_default();
//             let age: Option<i32> = row.try_get("age").ok();

//             info!(
//                 "  [{}] HN={}, VN={}, Disease={}, Age={}",
//                 idx + 1,
//                 hn,
//                 vn,
//                 disease,
//                 age.unwrap_or(0)
//             );
//         }
//         info!("");
//         info!("════════════════════════════════════════════════");

//         Ok(())
//     }
// }

// // ============================================================================
// // L2.5: HEALTH CHECK SUBSYSTEM
// // ============================================================================

// mod health_checker {
//     use super::*;

//     pub async fn run_health_check(
//         src_pool: &MySqlPool,
//         dst_pool: &MySqlPool,
//         config: &SyncConfig,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         info!("");
//         info!("🏥 === COMPREHENSIVE HEALTH CHECK ===");
//         info!("════════════════════════════════════════════════");

//         let tables = vec!["opdscreen", "vn_stat", "icd101", "opitemrece", "drugitems", "hismember"];

//         info!("");
//         info!("📋 Source Database Table Status:");
//         for table in &tables {
//             let sql = format!("SELECT COUNT(*) as cnt FROM `{}`.{}", config.src_db, table);
//             match sqlx::query_as::<_, (i64,)>(&sql).fetch_one(src_pool).await {
//                 Ok((count,)) => {
//                     if count > 0 {
//                         info!("  ✅ {}.{}: {} records", config.src_db, table, count);
//                     } else {
//                         warn!("  ⚠️ {}.{}: EMPTY", config.src_db, table);
//                     }
//                 }
//                 Err(e) => {
//                     error!("  ❌ {}.{}: {}", config.src_db, table, e);
//                 }
//             }
//         }

//         info!("");
//         info!("📋 Destination Database Status:");
//         let dst_sql = format!("SELECT COUNT(*) as cnt FROM `{}`.`ai_disease_training_data`", config.dst_db);
//         match sqlx::query_as::<_, (i64,)>(&dst_sql).fetch_one(dst_pool).await {
//             Ok((count,)) => {
//                 info!("  ✅ {}.ai_disease_training_data: {} records", config.dst_db, count);
//             }
//             Err(e) => {
//                 error!("  ❌ {}.ai_disease_training_data: {}", config.dst_db, e);
//             }
//         }

//         info!("");
//         info!("✅ Health check completed");
//         info!("════════════════════════════════════════════════");
//         info!("");

//         Ok(())
//     }
// }

// // ============================================================================
// // L2.6: VERIFICATION SUBSYSTEM
// // ============================================================================

// mod verifier {
//     use super::*;

//     pub async fn verify_data_integrity(
//         pool: &MySqlPool,
//         config: &SyncConfig,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         info!("");
//         info!("🔍 === DATA INTEGRITY VERIFICATION ===");
//         info!("════════════════════════════════════════════════");

//         let checks = vec![
//             (
//                 "Total Records",
//                 format!("SELECT COUNT(*) FROM `{}`.`ai_disease_training_data`", config.dst_db),
//             ),
//             (
//                 "Unique Patients (HN)",
//                 format!("SELECT COUNT(DISTINCT hn) FROM `{}`.`ai_disease_training_data` WHERE hn IS NOT NULL", config.dst_db),
//             ),
//             (
//                 "Unique Diseases (ICD10)",
//                 format!("SELECT COUNT(DISTINCT icd10_code) FROM `{}`.`ai_disease_training_data` WHERE icd10_code != 'Unknown'", config.dst_db),
//             ),
//             (
//                 "Records with Unknown Symptoms",
//                 format!("SELECT COUNT(*) FROM `{}`.`ai_disease_training_data` WHERE symptoms = 'Unknown'", config.dst_db),
//             ),
//             (
//                 "Records with Unknown Disease",
//                 format!("SELECT COUNT(*) FROM `{}`.`ai_disease_training_data` WHERE disease_name = 'Unknown'", config.dst_db),
//             ),
//             (
//                 "Average Age",
//                 format!("SELECT ROUND(AVG(age), 1) FROM `{}`.`ai_disease_training_data` WHERE age > 0", config.dst_db),
//             ),
//         ];

//         for (label, sql) in checks {
//             match sqlx::query(&sql).fetch_one(pool).await {
//                 Ok(row) => {
//                     let value: Option<String> = row.try_get(0).ok();
//                     info!("  ✅ {}: {}", label, value.unwrap_or_else(|| "N/A".to_string()));
//                 }
//                 Err(e) => {
//                     error!("  ❌ {}: {}", label, e);
//                 }
//             }
//         }

//         info!("");
//         info!("════════════════════════════════════════════════");
//         Ok(())
//     }
// }

// // ============================================================================
// // L2.7: PERFORMANCE MONITORING SUBSYSTEM
// // ============================================================================

// impl PerformanceMonitor {
//     fn new() -> Self {
//         PerformanceMonitor {
//             start_time: std::time::Instant::now(),
//             checkpoints: Arc::new(Mutex::new(Vec::new())),
//         }
//     }

//     fn checkpoint(&self, label: &str) {
//         if let Ok(mut checkpoints) = self.checkpoints.lock() {
//             checkpoints.push((label.to_string(), std::time::Instant::now()));
//             info!(
//                 "⏱️ [{}] Elapsed: {:.2}s",
//                 label,
//                 self.start_time.elapsed().as_secs_f64()
//             );
//         }
//     }

//     fn report(&self) {
//         info!("");
//         info!("⏱️ === PERFORMANCE REPORT ===");
//         info!("🏁 Total execution time: {:.2}s", self.start_time.elapsed().as_secs_f64());

//         if let Ok(checkpoints) = self.checkpoints.lock() {
//             for (i, (label, _)) in checkpoints.iter().enumerate() {
//                 info!("  [{}] {}", i + 1, label);
//             }
//         }
//         info!("════════════════════════════════════════════════");
//         info!("");
//     }
// }

// // ============================================================================
// // L2.8: CLI PARSER SUBSYSTEM
// // ============================================================================

// mod cli_parser {
//     use super::*;

//     pub fn parse_arguments() -> SyncMode {
//         let args: Vec<String> = std::env::args().collect();

//         if args.len() > 1 {
//             match args[1].as_str() {
//                 "incremental" => {
//                     let hours = if args.len() > 2 {
//                         args[2].parse::<i32>().unwrap_or(24)
//                     } else {
//                         24
//                     };
//                     SyncMode::Incremental(hours)
//                 }
//                 "health" => SyncMode::HealthCheck,
//                 "preview" => SyncMode::Preview,
//                 "verify" => SyncMode::Verify,
//                 "--help" | "-h" => {
//                     print_help();
//                     std::process::exit(0);
//                 }
//                 _ => {
//                     println!("Unknown command: {}", args[1]);
//                     print_help();
//                     std::process::exit(1);
//                 }
//             }
//         } else {
//             SyncMode::Full
//         }
//     }

//     fn print_help() {
//         println!("");
//         println!("🚀 AI DISEASE TRAINING DATA SYNC");
//         println!("");
//         println!("Usage: ./sync [COMMAND]");
//         println!("");
//         println!("Commands:");
//         println!("  (none)          Full sync - syncs all data");
//         println!("  incremental [N] Incremental sync - syncs last N hours (default: 24)");
//         println!("  health          Run health checks");
//         println!("  preview         Preview sample data");
//         println!("  verify          Verify data integrity");
//         println!("  --help, -h      Show this help message");
//         println!("");
//         println!("Examples:");
//         println!("  ./sync                          # Full sync");
//         println!("  ./sync incremental              # Last 24 hours");
//         println!("  ./sync incremental 72           # Last 72 hours");
//         println!("  ./sync health                   # Health check");
//         println!("  ./sync preview                  # Preview data");
//         println!("");
//     }
// }

// // ============================================================================
// // L1: MAIN APPLICATION - ORCHESTRATION
// // ============================================================================

// #[tokio::main(flavor = "multi_thread")]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     logger_system::init_logger()?;

//     let cpu_cores = num_cpus::get();
//     let max_workers = (cpu_cores - 1).max(2);

//     info!("");
//     info!("🚀 AI DISEASE TRAINING DATA SYNC - Direct SQL INSERT");
//     info!("⚙️ CPU Cores: {}", cpu_cores);
//     info!("👥 Workers: {}", max_workers);
//     info!("⏰ Started: {}", Local::now().format("%Y-%m-%d %H:%M:%S"));

//     let perf = PerformanceMonitor::new();

//     let config = SyncConfig {
//         db_src: std::env::var("DB_SRC")
//             .unwrap_or_else(|_| "mysql://root:root@localhost:3306".to_string()),
//         db_dst: std::env::var("DB_DST")
//             .unwrap_or_else(|_| "mysql://root:root@localhost:3306".to_string()),
//         src_db: "hos".to_string(),
//         dst_db: "hos_ai".to_string(),
//         batch_size: 500,
//         limit: 50000,
//         max_workers,
//     };

//     perf.checkpoint("Configuration loaded");

//     let mode = cli_parser::parse_arguments();

//     info!("📋 Sync Mode: {:?}", mode);

//     perf.checkpoint("CLI parsed");

//     // Create connection pools
//     let src_pool = connection_manager::create_pool(&config.db_src, 10, "SOURCE").await?;
//     perf.checkpoint("Source pool created");

//     let dst_pool = connection_manager::create_pool(&config.db_dst, 10, "DESTINATION").await?;
//     perf.checkpoint("Destination pool created");

//     // Verify connections
//     connection_manager::verify_connection(&src_pool, &config.src_db).await?;
//     connection_manager::verify_connection(&dst_pool, &config.dst_db).await?;
//     perf.checkpoint("Connections verified");

//     // Create destination table
//     table_manager::create_training_table(&dst_pool, &config.dst_db).await?;
//     perf.checkpoint("Training table created");

//     // Execute based on mode
//     match mode {
//         SyncMode::Full => {
//             info!("");
//             info!("📊 === FULL SYNC MODE ===");
//             info!("");
            
//             table_manager::clear_table(&dst_pool, &config.dst_db).await?;
//             perf.checkpoint("Table cleared");

//             match sql_executor::execute_full_sync(&src_pool, &dst_pool, &config).await {
//                 Ok(stats) => {
//                     info!("");
//                     info!("✅ SYNC COMPLETED SUCCESSFULLY");
//                     info!("📊 Total Processed: {}", stats.total_processed);
//                     info!("✏️ Total Inserted: {}", stats.total_inserted);
//                     info!("❌ Total Errors: {}", stats.total_errors);
//                     info!("⏱️ Execution Time: {:.2}s", stats.execution_time);
//                     info!("");
//                 }
//                 Err(e) => {
//                     error!("❌ SYNC FAILED: {}", e);
//                     return Err(e);
//                 }
//             }
//         }
//         SyncMode::Incremental(hours) => {
//             info!("");
//             info!("🔄 === INCREMENTAL SYNC MODE (last {} hours) ===", hours);
//             info!("");

//             match sql_executor::execute_incremental_sync(&src_pool, &dst_pool, &config, hours).await {
//                 Ok(stats) => {
//                     info!("");
//                     info!("✅ INCREMENTAL SYNC COMPLETED");
//                     info!("📊 Total Processed: {}", stats.total_processed);
//                     info!("✏️ Total Inserted: {}", stats.total_inserted);
//                     info!("⏱️ Execution Time: {:.2}s", stats.execution_time);
//                     info!("");
//                 }
//                 Err(e) => {
//                     error!("❌ INCREMENTAL SYNC FAILED: {}", e);
//                     return Err(e);
//                 }
//             }
//         }
//         SyncMode::HealthCheck => {
//             health_checker::run_health_check(&src_pool, &dst_pool, &config).await?;
//         }
//         SyncMode::Preview => {
//             sql_executor::preview_data(&src_pool, &config).await?;
//         }
//         SyncMode::Verify => {
//             verifier::verify_data_integrity(&dst_pool, &config).await?;
//         }
//     }

//     perf.checkpoint("Mode execution completed");
//     perf.report();

//     info!("🏁 AI Disease Training Data Sync - FINISHED");
//     info!("⏰ Completed: {}", Local::now().format("%Y-%m-%d %H:%M:%S"));
//     info!("");

//     Ok(())
// }

// ============================================================================
// FILE: src/main.rs
// AI DISEASE TRAINING DATA SYNC - COMPLETE PROJECT
// Direct SQL INSERT with Complex JOIN Query
// UPDATED: .env Configuration Support
// ============================================================================
use sqlx::{MySqlPool, Row, mysql::MySqlPoolOptions};
use log::{info, warn, error, debug};
use chrono::Local;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use flexi_logger::{Logger, FileSpec, WriteMode, Criterion};
use num_cpus;
use std::fs;
use dotenv::dotenv;

// ============================================================================
// L1: DATA STRUCTURES & CONSTANTS
// ============================================================================
#[derive(Debug, Clone)]
struct SyncConfig {
    db_src: String,
    db_dst: String,
    src_db: String,
    dst_db: String,
    batch_size: usize,
    limit: usize,
    max_workers: usize,
}

#[derive(Debug, Clone)]
struct SyncStats {
    total_processed: usize,
    total_inserted: usize,
    total_errors: usize,
    total_duration: Duration,
    execution_time: f64,
}

#[derive(Debug)]
struct PerformanceMonitor {
    start_time: std::time::Instant,
    checkpoints: Arc<Mutex<Vec<(String, std::time::Instant)>>>,
}

#[derive(Debug)]
enum SyncMode {
    Full,
    Incremental(i32),
    HealthCheck,
    Preview,
    Verify,
}

// ============================================================================
// L2.0: ENVIRONMENT CONFIGURATION SUBSYSTEM
// ============================================================================
mod env_config {
    use super::*;

    pub struct EnvConfig {
        pub db_src_host: String,
        pub db_src_port: u16,
        pub db_src_user: String,
        pub db_src_pass: String,
        pub db_dst_host: String,
        pub db_dst_port: u16,
        pub db_dst_user: String,
        pub db_dst_pass: String,
        pub src_db: String,
        pub dst_db: String,
    }

    impl EnvConfig {
        pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
            dotenv().ok();

            let config = EnvConfig {
                db_src_host: std::env::var("DB_SRC_HOST")
                    .unwrap_or_else(|_| "localhost".to_string()),
                db_src_port: std::env::var("DB_SRC_PORT")
                    .unwrap_or_else(|_| "3306".to_string())
                    .parse()
                    .unwrap_or(3306),
                db_src_user: std::env::var("DB_SRC_USER")
                    .unwrap_or_else(|_| "root".to_string()),
                db_src_pass: std::env::var("DB_SRC_PASS")
                    .unwrap_or_else(|_| "root".to_string()),
                db_dst_host: std::env::var("DB_DST_HOST")
                    .unwrap_or_else(|_| "localhost".to_string()),
                db_dst_port: std::env::var("DB_DST_PORT")
                    .unwrap_or_else(|_| "3306".to_string())
                    .parse()
                    .unwrap_or(3306),
                db_dst_user: std::env::var("DB_DST_USER")
                    .unwrap_or_else(|_| "root".to_string()),
                db_dst_pass: std::env::var("DB_DST_PASS")
                    .unwrap_or_else(|_| "root".to_string()),
                src_db: std::env::var("SRC_DATABASE")
                    .unwrap_or_else(|_| "hos".to_string()),
                dst_db: std::env::var("DST_DATABASE")
                    .unwrap_or_else(|_| "hos_ai".to_string()),
            };

            Ok(config)
        }

        pub fn build_src_connection_string(&self) -> String {
            format!(
                "mysql://{}:{}@{}:{}/",
                self.db_src_user,
                self.db_src_pass,
                self.db_src_host,
                self.db_src_port
            )
        }

        pub fn build_dst_connection_string(&self) -> String {
            format!(
                "mysql://{}:{}@{}:{}/",
                self.db_dst_user,
                self.db_dst_pass,
                self.db_dst_host,
                self.db_dst_port
            )
        }

        pub fn log_config(&self) {
            info!("📋 === DATABASE CONFIGURATION ===");
            info!("🔵 Source Database:");
            info!("   Host: {}", self.db_src_host);
            info!("   Port: {}", self.db_src_port);
            info!("   User: {}", self.db_src_user);
            info!("   Database: {}", self.src_db);
            info!("🔴 Destination Database:");
            info!("   Host: {}", self.db_dst_host);
            info!("   Port: {}", self.db_dst_port);
            info!("   User: {}", self.db_dst_user);
            info!("   Database: {}", self.dst_db);
            info!("════════════════════════════════════════════════");
        }
    }
}

// ============================================================================
// L2.1: LOGGER SUBSYSTEM - ENHANCED
// ============================================================================
mod logger_system {
    use super::*;

    pub fn init_logger() -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all("logs")?;
        Logger::try_with_str("debug")?
            .log_to_file(
                FileSpec::default()
                    .directory("logs")
                    .basename("disease_sync"),
            )
            .log_to_stderr()
            .duplicate_to_stderr(flexi_logger::Duplicate::All)
            .write_mode(WriteMode::BufferAndFlush)
            .rotate(
                Criterion::Size(10_485_760),
                flexi_logger::Naming::Numbers,
                flexi_logger::Cleanup::KeepLogFiles(5),
            )
            .format(|w, now, record| {
                write!(
                    w,
                    "[{}] [{}] {}",
                    now.format("%Y-%m-%d %H:%M:%S%.3f"),
                    record.level(),
                    &record.args()
                )
            })
            .start()?;
        info!("════════════════════════════════════════════════");
        info!("✅ Logger initialized - File: logs/disease_sync_*.log");
        info!("════════════════════════════════════════════════");
        Ok(())
    }
}

// ============================================================================
// L2.2: CONNECTION MANAGER SUBSYSTEM
// ============================================================================
mod connection_manager {
    use super::*;

    pub async fn create_pool(
        connection_string: &str,
        max_connections: u32,
        pool_name: &str,
    ) -> Result<MySqlPool, Box<dyn std::error::Error>> {
        debug!(
            "📡 Creating connection pool '{}' with max_connections={}",
            pool_name, max_connections
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(1800))
            .connect(connection_string)
            .await?;
        info!("✅ Connection pool '{}' created successfully", pool_name);
        debug!(
            " Connection string: {}",
            mask_connection_string(connection_string)
        );
        Ok(pool)
    }

    pub async fn verify_connection(
        pool: &MySqlPool,
        db_name: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        debug!("🔍 Verifying connection to database: {}", db_name);
        match sqlx::query_as::<_, (i32,)>("SELECT 1").fetch_one(pool).await {
            Ok((result,)) => {
                info!("✅ Database connection verified for: {}", db_name);
                Ok(result == 1)
            }
            Err(e) => {
                error!("❌ Connection verification failed for {}: {}", db_name, e);
                Err(Box::new(e))
            }
        }
    }

    pub async fn get_source_record_count(
        pool: &MySqlPool,
        src_db: &str,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let sql = format!("SELECT COUNT(*) FROM `{}`.opdscreen", src_db);
        let result: (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
        debug!("Source record count from {}.opdscreen: {}", src_db, result.0);
        Ok(result.0)
    }

    fn mask_connection_string(conn_str: &str) -> String {
        conn_str
            .split(':')
            .enumerate()
            .map(|(i, part)| {
                if i == 1 {
                    "***".to_string()
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(":")
    }
}

// ============================================================================
// L2.3: TABLE MANAGEMENT SUBSYSTEM
// ============================================================================
mod table_manager {
    use super::*;

    pub async fn create_training_table(
        pool: &MySqlPool,
        db_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("📋 Creating training data table in {}...", db_name);
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS `{db_name}`.`ai_disease_training_data` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `visit_id` VARCHAR(50) UNIQUE NOT NULL,
                `hn` VARCHAR(9),
                `vn` VARCHAR(13),
                `symptoms` LONGTEXT,
                `icd10_code` VARCHAR(9),
                `disease_name` VARCHAR(255),
                `medicines` LONGTEXT,
                `age` INT,
                `sex` CHAR(1),
                `visit_date` DATE,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX `idx_hn` (`hn`),
                INDEX `idx_vn` (`vn`),
                INDEX `idx_icd10` (`icd10_code`),
                INDEX `idx_visit_date` (`visit_date`),
                INDEX `idx_age` (`age`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            db_name = db_name
        );
        info!(
            "📋 Creating/Verifying table: {}.ai_disease_training_data",
            db_name
        );
        match sqlx::query(&create_table_sql).execute(pool).await {
            Ok(_) => {
                info!("✅ Table created/verified successfully");
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to create table: {}", e);
                Err(Box::new(e))
            }
        }
    }

    pub async fn clear_table(
        pool: &MySqlPool,
        db_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("🗑️ Clearing existing training data table...");
        let truncate_sql = format!("TRUNCATE TABLE `{}`.`ai_disease_training_data`", db_name);
        match sqlx::query(&truncate_sql).execute(pool).await {
            Ok(result) => {
                info!(
                    "✅ Table cleared - Rows affected: {}",
                    result.rows_affected()
                );
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to clear table: {}", e);
                Err(Box::new(e))
            }
        }
    }

    pub async fn get_table_count(
        pool: &MySqlPool,
        db_name: &str,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let sql = format!(
            "SELECT COUNT(*) as cnt FROM `{}`.`ai_disease_training_data`",
            db_name
        );
        let result: (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
        debug!("Current table count: {}", result.0);
        Ok(result.0)
    }
}

// ============================================================================
// L2.4: DIRECT SQL EXECUTION SUBSYSTEM (Main Logic)
// ============================================================================
mod sql_executor {
    use super::*;

    fn get_insert_query(src_db: &str, dst_db: &str) -> String {
        format!(
            r#"
            INSERT INTO `{dst_db}`.`ai_disease_training_data`
            (visit_id, hn, vn, symptoms, icd10_code, disease_name, medicines, age, sex, visit_date)
            SELECT
                CONCAT(o.hn, '-', o.vn) as visit_id,
                o.hn,
                o.vn,
                COALESCE(o.cc, 'Unknown') as symptoms,
                COALESCE(i.code, 'Unknown') as icd10_code,
                COALESCE(i.name, 'Unknown') as disease_name,
                COALESCE(GROUP_CONCAT(DISTINCT CONCAT(d.name, ' ', COALESCE(d.strength, '')) SEPARATOR '|'), 'Unknown') as medicines,
                YEAR(CURDATE()) - YEAR(COALESCE(o.vstdate, CURDATE())) as age,
                COALESCE(h.sex, 'U') as sex,
                o.vstdate as visit_date
            FROM `{src_db}`.opdscreen o
            LEFT JOIN `{src_db}`.vn_stat v ON v.vn = o.vn
            LEFT JOIN `{src_db}`.icd101 i ON i.code = v.pdx
            LEFT JOIN `{src_db}`.opitemrece op ON op.vn = o.vn
            LEFT JOIN `{src_db}`.drugitems d ON d.icode = op.icode
            LEFT JOIN `{src_db}`.hismember h ON h.hn = o.hn
            WHERE i.code IS NOT NULL
            AND TRIM(COALESCE(v.pdx, '')) != ''
            GROUP BY o.hn, o.vn, i.code, o.vstdate
            ORDER BY o.vstdate DESC
            LIMIT ?
            "#,
            src_db = src_db,
            dst_db = dst_db
        )
    }

    pub async fn execute_full_sync(
        src_pool: &MySqlPool,
        dst_pool: &MySqlPool,
        config: &SyncConfig,
    ) -> Result<SyncStats, Box<dyn std::error::Error>> {
        info!("🚀 Starting FULL SYNC with direct SQL INSERT...");
        info!("════════════════════════════════════════════════");
        let start_time = std::time::Instant::now();
        let insert_sql = get_insert_query(&config.src_db, &config.dst_db);
        info!("📊 Building complex JOIN query...");
        info!("🔗 Tables involved: opdscreen, vn_stat, icd101, opitemrece, drugitems, hismember");
        info!("📦 Processing up to {} records", config.limit);
        debug!(
            "Generated SQL: {}",
            insert_sql.lines().take(5).collect::<Vec<_>>().join(" ")
        );

        let source_count =
            connection_manager::get_source_record_count(src_pool, &config.src_db).await?;
        info!("✅ Source opdscreen has {} records", source_count);

        if source_count == 0 {
            warn!("⚠️ No source data found");
            return Ok(SyncStats {
                total_processed: 0,
                total_inserted: 0,
                total_errors: 0,
                total_duration: start_time.elapsed(),
                execution_time: 0.0,
            });
        }

        info!("💾 Executing INSERT INTO...SELECT with JOINs...");
        match sqlx::query(&insert_sql)
            .bind(config.limit as u32)
            .execute(dst_pool)
            .await
        {
            Ok(result) => {
                let rows_affected = result.rows_affected() as usize;
                info!("✅ Query executed successfully");
                info!("📈 Rows affected: {}", rows_affected);
                let final_count =
                    table_manager::get_table_count(dst_pool, &config.dst_db).await?;
                info!("✅ Final record count in destination: {}", final_count);
                info!("════════════════════════════════════════════════");
                let duration = start_time.elapsed();
                let execution_time = duration.as_secs_f64();
                Ok(SyncStats {
                    total_processed: rows_affected,
                    total_inserted: rows_affected,
                    total_errors: 0,
                    total_duration: duration,
                    execution_time,
                })
            }
            Err(e) => {
                error!("❌ Query execution failed: {}", e);
                error!(
                    "SQL (first 500 chars): {}",
                    &insert_sql[..insert_sql.len().min(500)]
                );
                Err(Box::new(e))
            }
        }
    }

    pub async fn execute_incremental_sync(
        src_pool: &MySqlPool,
        dst_pool: &MySqlPool,
        config: &SyncConfig,
        hours: i32,
    ) -> Result<SyncStats, Box<dyn std::error::Error>> {
        info!("🔄 Starting INCREMENTAL SYNC (last {} hours)...", hours);
        info!("════════════════════════════════════════════════");
        let start_time = std::time::Instant::now();
        let incremental_sql = format!(
            r#"
            INSERT INTO `{dst_db}`.`ai_disease_training_data`
            (visit_id, hn, vn, symptoms, icd10_code, disease_name, medicines, age, sex, visit_date)
            SELECT
                CONCAT(o.hn, '-', o.vn) as visit_id,
                o.hn,
                o.vn,
                COALESCE(o.cc, 'Unknown') as symptoms,
                COALESCE(i.code, 'Unknown') as icd10_code,
                COALESCE(i.name, 'Unknown') as disease_name,
                COALESCE(GROUP_CONCAT(DISTINCT CONCAT(d.name, ' ', COALESCE(d.strength, '')) SEPARATOR '|'), 'Unknown') as medicines,
                YEAR(CURDATE()) - YEAR(COALESCE(o.vstdate, CURDATE())) as age,
                COALESCE(h.sex, 'U') as sex,
                o.vstdate as visit_date
            FROM `{src_db}`.opdscreen o
            LEFT JOIN `{src_db}`.vn_stat v ON v.vn = o.vn
            LEFT JOIN `{src_db}`.icd101 i ON i.code = v.pdx
            LEFT JOIN `{src_db}`.opitemrece op ON op.vn = o.vn
            LEFT JOIN `{src_db}`.drugitems d ON d.icode = op.icode
            LEFT JOIN `{src_db}`.hismember h ON h.hn = o.hn
            WHERE i.code IS NOT NULL
            AND TRIM(COALESCE(v.pdx, '')) != ''
            AND o.vstdate >= DATE_SUB(NOW(), INTERVAL ? HOUR)
            GROUP BY o.hn, o.vn, i.code, o.vstdate
            ON DUPLICATE KEY UPDATE
                symptoms = VALUES(symptoms),
                disease_name = VALUES(disease_name),
                medicines = VALUES(medicines),
                age = VALUES(age)
            "#,
            src_db = config.src_db,
            dst_db = config.dst_db
        );
        info!("⏰ Syncing data from last {} hours", hours);
        info!("🔍 Checking for recent updates...");
        match sqlx::query(&incremental_sql)
            .bind(hours)
            .execute(dst_pool)
            .await
        {
            Ok(result) => {
                let rows_affected = result.rows_affected() as usize;
                info!("✅ Incremental sync completed");
                info!("📈 Rows affected: {}", rows_affected);
                info!("════════════════════════════════════════════════");
                let duration = start_time.elapsed();
                Ok(SyncStats {
                    total_processed: rows_affected,
                    total_inserted: rows_affected,
                    total_errors: 0,
                    total_duration: duration,
                    execution_time: duration.as_secs_f64(),
                })
            }
            Err(e) => {
                error!("❌ Incremental sync failed: {}", e);
                Err(Box::new(e))
            }
        }
    }

    pub async fn preview_data(
        src_pool: &MySqlPool,
        config: &SyncConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("👁️ Previewing first 10 records from source query...");
        info!("════════════════════════════════════════════════");
        let preview_sql = format!(
            r#"
            SELECT
                CONCAT(o.hn, '-', o.vn) as visit_id,
                o.hn,
                o.vn,
                COALESCE(o.cc, 'Unknown') as symptoms,
                COALESCE(i.code, 'Unknown') as icd10_code,
                COALESCE(i.name, 'Unknown') as disease_name,
                COALESCE(GROUP_CONCAT(DISTINCT CONCAT(d.name, ' ', COALESCE(d.strength, '')) SEPARATOR '|'), 'Unknown') as medicines,
                YEAR(CURDATE()) - YEAR(COALESCE(o.vstdate, CURDATE())) as age,
                COALESCE(h.sex, 'U') as sex,
                o.vstdate as visit_date
            FROM `{src_db}`.opdscreen o
            LEFT JOIN `{src_db}`.vn_stat v ON v.vn = o.vn
            LEFT JOIN `{src_db}`.icd101 i ON i.code = v.pdx
            LEFT JOIN `{src_db}`.opitemrece op ON op.vn = o.vn
            LEFT JOIN `{src_db}`.drugitems d ON d.icode = op.icode
            LEFT JOIN `{src_db}`.hismember h ON h.hn = o.hn
            WHERE i.code IS NOT NULL
            AND TRIM(COALESCE(v.pdx, '')) != ''
            GROUP BY o.hn, o.vn, i.code, o.vstdate
            ORDER BY o.vstdate DESC
            LIMIT 10
            "#,
            src_db = config.src_db
        );
        let rows = sqlx::query(&preview_sql).fetch_all(src_pool).await?;
        info!("📊 Preview: {} records found", rows.len());
        info!("");
        for (idx, row) in rows.iter().enumerate() {
            let visit_id: String = row.try_get("visit_id").unwrap_or_default();
            let hn: String = row.try_get("hn").unwrap_or_default();
            let vn: String = row.try_get("vn").unwrap_or_default();
            let disease: String = row.try_get("disease_name").unwrap_or_default();
            let age: Option<i32> = row.try_get("age").ok();
            info!(
                " [{}] HN={}, VN={}, Disease={}, Age={}",
                idx + 1,
                hn,
                vn,
                disease,
                age.unwrap_or(0)
            );
        }
        info!("");
        info!("════════════════════════════════════════════════");
        Ok(())
    }
}

// ============================================================================
// L2.5: HEALTH CHECK SUBSYSTEM
// ============================================================================
mod health_checker {
    use super::*;

    pub async fn run_health_check(
        src_pool: &MySqlPool,
        dst_pool: &MySqlPool,
        config: &SyncConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("");
        info!("🏥 === COMPREHENSIVE HEALTH CHECK ===");
        info!("════════════════════════════════════════════════");
        let tables = vec!["opdscreen", "vn_stat", "icd101", "opitemrece", "drugitems", "hismember"];
        info!("");
        info!("📋 Source Database Table Status:");
        for table in &tables {
            let sql = format!(
                "SELECT COUNT(*) as cnt FROM `{}`.{}",
                config.src_db, table
            );
            match sqlx::query_as::<_, (i64,)>(&sql).fetch_one(src_pool).await {
                Ok((count,)) => {
                    if count > 0 {
                        info!(" ✅ {}.{}: {} records", config.src_db, table, count);
                    } else {
                        warn!(" ⚠️ {}.{}: EMPTY", config.src_db, table);
                    }
                }
                Err(e) => {
                    error!(" ❌ {}.{}: {}", config.src_db, table, e);
                }
            }
        }
        info!("");
        info!("📋 Destination Database Status:");
        let dst_sql = format!(
            "SELECT COUNT(*) as cnt FROM `{}`.`ai_disease_training_data`",
            config.dst_db
        );
        match sqlx::query_as::<_, (i64,)>(&dst_sql).fetch_one(dst_pool).await {
            Ok((count,)) => {
                info!(
                    " ✅ {}.ai_disease_training_data: {} records",
                    config.dst_db, count
                );
            }
            Err(e) => {
                error!(
                    " ❌ {}.ai_disease_training_data: {}",
                    config.dst_db, e
                );
            }
        }
        info!("");
        info!("✅ Health check completed");
        info!("════════════════════════════════════════════════");
        info!("");
        Ok(())
    }
}

// ============================================================================
// L2.6: VERIFICATION SUBSYSTEM
// ============================================================================
mod verifier {
    use super::*;

    pub async fn verify_data_integrity(
        pool: &MySqlPool,
        config: &SyncConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("");
        info!("🔍 === DATA INTEGRITY VERIFICATION ===");
        info!("════════════════════════════════════════════════");
        let checks = vec![
            (
                "Total Records",
                format!("SELECT COUNT(*) FROM `{}`.`ai_disease_training_data`", config.dst_db),
            ),
            (
                "Unique Patients (HN)",
                format!(
                    "SELECT COUNT(DISTINCT hn) FROM `{}`.`ai_disease_training_data` WHERE hn IS NOT NULL",
                    config.dst_db
                ),
            ),
            (
                "Unique Diseases (ICD10)",
                format!(
                    "SELECT COUNT(DISTINCT icd10_code) FROM `{}`.`ai_disease_training_data` WHERE icd10_code != 'Unknown'",
                    config.dst_db
                ),
            ),
            (
                "Records with Unknown Symptoms",
                format!(
                    "SELECT COUNT(*) FROM `{}`.`ai_disease_training_data` WHERE symptoms = 'Unknown'",
                    config.dst_db
                ),
            ),
            (
                "Records with Unknown Disease",
                format!(
                    "SELECT COUNT(*) FROM `{}`.`ai_disease_training_data` WHERE disease_name = 'Unknown'",
                    config.dst_db
                ),
            ),
            (
                "Average Age",
                format!(
                    "SELECT ROUND(AVG(age), 1) FROM `{}`.`ai_disease_training_data` WHERE age > 0",
                    config.dst_db
                ),
            ),
        ];
        for (label, sql) in checks {
            match sqlx::query(&sql).fetch_one(pool).await {
                Ok(row) => {
                    let value: Option<String> = row.try_get(0).ok();
                    info!(" ✅ {}: {}", label, value.unwrap_or_else(|| "N/A".to_string()));
                }
                Err(e) => {
                    error!(" ❌ {}: {}", label, e);
                }
            }
        }
        info!("");
        info!("════════════════════════════════════════════════");
        Ok(())
    }
}

// ============================================================================
// L2.7: PERFORMANCE MONITORING SUBSYSTEM
// ============================================================================
impl PerformanceMonitor {
    fn new() -> Self {
        PerformanceMonitor {
            start_time: std::time::Instant::now(),
            checkpoints: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn checkpoint(&self, label: &str) {
        if let Ok(mut checkpoints) = self.checkpoints.lock() {
            checkpoints.push((label.to_string(), std::time::Instant::now()));
            info!(
                "⏱️ [{}] Elapsed: {:.2}s",
                label,
                self.start_time.elapsed().as_secs_f64()
            );
        }
    }

    fn report(&self) {
        info!("");
        info!("⏱️ === PERFORMANCE REPORT ===");
        info!(
            "🏁 Total execution time: {:.2}s",
            self.start_time.elapsed().as_secs_f64()
        );
        if let Ok(checkpoints) = self.checkpoints.lock() {
            for (i, (label, _)) in checkpoints.iter().enumerate() {
                info!(" [{}] {}", i + 1, label);
            }
        }
        info!("════════════════════════════════════════════════");
        info!("");
    }
}

// ============================================================================
// L2.8: CLI PARSER SUBSYSTEM
// ============================================================================
mod cli_parser {
    use super::*;

    pub fn parse_arguments() -> SyncMode {
        let args: Vec<String> = std::env::args().collect();
        if args.len() > 1 {
            match args[1].as_str() {
                "incremental" => {
                    let hours = if args.len() > 2 {
                        args[2].parse::<i32>().unwrap_or(24)
                    } else {
                        24
                    };
                    SyncMode::Incremental(hours)
                }
                "health" => SyncMode::HealthCheck,
                "preview" => SyncMode::Preview,
                "verify" => SyncMode::Verify,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => {
                    println!("Unknown command: {}", args[1]);
                    print_help();
                    std::process::exit(1);
                }
            }
        } else {
            SyncMode::Full
        }
    }

    fn print_help() {
        println!("");
        println!("🚀 AI DISEASE TRAINING DATA SYNC");
        println!("");
        println!("Usage: ./sync [COMMAND]");
        println!("");
        println!("Commands:");
        println!(" (none)           Full sync - syncs all data");
        println!(" incremental [N]  Incremental sync - syncs last N hours (default: 24)");
        println!(" health           Run health checks");
        println!(" preview          Preview sample data");
        println!(" verify           Verify data integrity");
        println!(" --help, -h       Show this help message");
        println!("");
        println!("Examples:");
        println!(" ./sync                    # Full sync");
        println!(" ./sync incremental        # Last 24 hours");
        println!(" ./sync incremental 72     # Last 72 hours");
        println!(" ./sync health             # Health check");
        println!(" ./sync preview            # Preview data");
        println!("");
    }
}

// ============================================================================
// L1: MAIN APPLICATION - ORCHESTRATION
// ============================================================================
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logger_system::init_logger()?;

    let cpu_cores = num_cpus::get();
    let max_workers = (cpu_cores - 1).max(2);

    info!("");
    info!("🚀 AI DISEASE TRAINING DATA SYNC - Direct SQL INSERT");
    info!("⚙️ CPU Cores: {}", cpu_cores);
    info!("👥 Workers: {}", max_workers);
    info!("⏰ Started: {}", Local::now().format("%Y-%m-%d %H:%M:%S"));

    let perf = PerformanceMonitor::new();

    // Load environment configuration
    let env_config = env_config::EnvConfig::from_env()?;
    perf.checkpoint("Environment loaded");

    // Display configuration
    env_config.log_config();

    // Build connection strings
    let db_src = env_config.build_src_connection_string();
    let db_dst = env_config.build_dst_connection_string();

    let config = SyncConfig {
        db_src: db_src.clone(),
        db_dst: db_dst.clone(),
        src_db: env_config.src_db,
        dst_db: env_config.dst_db,
        batch_size: 500,
        limit: 50000,
        max_workers,
    };

    perf.checkpoint("Configuration loaded");

    let mode = cli_parser::parse_arguments();
    info!("📋 Sync Mode: {:?}", mode);
    perf.checkpoint("CLI parsed");

    // Create connection pools
    let src_pool = connection_manager::create_pool(&config.db_src, 10, "SOURCE").await?;
    perf.checkpoint("Source pool created");

    let dst_pool = connection_manager::create_pool(&config.db_dst, 10, "DESTINATION").await?;
    perf.checkpoint("Destination pool created");

    // Verify connections
    connection_manager::verify_connection(&src_pool, &config.src_db).await?;
    connection_manager::verify_connection(&dst_pool, &config.dst_db).await?;
    perf.checkpoint("Connections verified");

    // Create destination table
    table_manager::create_training_table(&dst_pool, &config.dst_db).await?;
    perf.checkpoint("Training table created");

    // Execute based on mode
    match mode {
        SyncMode::Full => {
            info!("");
            info!("📊 === FULL SYNC MODE ===");
            info!("");
            table_manager::clear_table(&dst_pool, &config.dst_db).await?;
            perf.checkpoint("Table cleared");

            match sql_executor::execute_full_sync(&src_pool, &dst_pool, &config).await {
                Ok(stats) => {
                    info!("");
                    info!("✅ SYNC COMPLETED SUCCESSFULLY");
                    info!("📊 Total Processed: {}", stats.total_processed);
                    info!("✏️ Total Inserted: {}", stats.total_inserted);
                    info!("❌ Total Errors: {}", stats.total_errors);
                    info!("⏱️ Execution Time: {:.2}s", stats.execution_time);
                    info!("");
                }
                Err(e) => {
                    error!("❌ SYNC FAILED: {}", e);
                    return Err(e);
                }
            }
        }
        SyncMode::Incremental(hours) => {
            info!("");
            info!("🔄 === INCREMENTAL SYNC MODE (last {} hours) ===", hours);
            info!("");
            match sql_executor::execute_incremental_sync(&src_pool, &dst_pool, &config, hours)
                .await
            {
                Ok(stats) => {
                    info!("");
                    info!("✅ INCREMENTAL SYNC COMPLETED");
                    info!("📊 Total Processed: {}", stats.total_processed);
                    info!("✏️ Total Inserted: {}", stats.total_inserted);
                    info!("⏱️ Execution Time: {:.2}s", stats.execution_time);
                    info!("");
                }
                Err(e) => {
                    error!("❌ INCREMENTAL SYNC FAILED: {}", e);
                    return Err(e);
                }
            }
        }
        SyncMode::HealthCheck => {
            health_checker::run_health_check(&src_pool, &dst_pool, &config).await?;
        }
        SyncMode::Preview => {
            sql_executor::preview_data(&src_pool, &config).await?;
        }
        SyncMode::Verify => {
            verifier::verify_data_integrity(&dst_pool, &config).await?;
        }
    }

    perf.checkpoint("Mode execution completed");
    perf.report();

    info!("🏁 AI Disease Training Data Sync - FINISHED");
    info!("⏰ Completed: {}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    info!("");

    Ok(())
}



