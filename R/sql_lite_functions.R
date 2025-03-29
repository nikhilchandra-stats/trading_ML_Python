connect_db <- function(path = "data/joined_lm_trig_data/raw_lm_trig.db"){

  # conn <- RSQLite::dbConnect(drv = RSQLite::SQLite(),path)

  conn <- DBI::dbConnect(RSQLite::SQLite(), path)

  return(conn)

}

write_table_sql_lite <- function(.data,
                                 table_name,
                                 conn,
                                 overwrite_true = TRUE){
  DBI::dbWriteTable(conn, table_name, .data, overwrite = overwrite_true)
}

append_table_sql_lite <- function(.data,
                                  table_name,
                                  conn){

  RSQLite::dbAppendTable(conn = conn,name = table_name,value = .data)

}
