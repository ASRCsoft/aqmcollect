## functions to update the raw data repository

## format units in column names
format_header = function(name, units) {
  paste0(name, ifelse(is.na(units), '', paste0(' (', units, ')')))
}

get_envidas_channels_nocache = function(con) {
  DBI::dbGetQuery(con, 'select number, name, units from Channel')
}

get_envidas_channels_cache = memoise::memoise(get_envidas_channels_nocache)

get_envidas_channels = function(con, use_cache = T) {
  if (use_cache) {
    get_envidas_channels_cache(con)
  } else {
    get_envidas_channels_nocache(con)
  }
}

#' Get data from Envidas.
#'
#' Retrieve a \code{data.frame} of Envidas data via Microsoft SQL
#' Server.
#' 
#' @param con A \code{DBIConnection} object for Envidas' SQL Server
#'   database.
#' @param site The site ID number.
#' @param agg_min The aggregation interval in minutes.
#' @param start The lower time bound (inclusive).
#' @param end The upper time bound (exclusive).
#' @return A \code{data.frame} of aggregated data for the available
#'   channels.
#' @export
get_envidas = function(con, site, agg_min, start, end) {
  ## get the processed data
  tbl_name = DBI::SQL(sprintf("S%03dT%02d", site, agg_min))
  sql0 = "SELECT *
            FROM ?tbl
           where Date_Time>=?start
             and Date_Time<?end
           order by Date_Time asc"
  sql = DBI::sqlInterpolate(con, sql0, tbl = tbl_name,
                            start = as.character(start),
                            end = as.character(end))
  res = DBI::dbGetQuery(con, sql)
  ## get channel info
  channels = get_envidas_channels(con)
  channels$value_name = format_header(channels$name, channels$units)
  channels$status_name = format_header(channels$name, 'status')
  col_names = names(res)
  col_channels =
    suppressWarnings(as.integer(sub('^Value|^Status', '', col_names)))
  ## remove unlabeled channels
  labeled = col_channels %in% channels$number
  keep_col = col_names == 'Date_Time' | labeled
  res = res[, keep_col]
  col_channels = col_channels[keep_col]
  ## rename columns
  col_names = names(res)
  is_value = grep('^Value', col_names)
  is_status = grep('^Status', col_names)
  names(res)[is_value] =
    channels$value_name[match(col_channels[is_value], channels$number)]
  names(res)[is_status] =
    channels$status_name[match(col_channels[is_status], channels$number)]
  res
}

#' @export
update_envidas = function(outdir, con, site, minutes, start_date,
                          end_date = Sys.Date()) {
  ## requested files
  dates = seq(start_date, end_date - 1, by = 'day')
  date_strs = format(dates, '%Y%m%d')
  req_file_names = paste(date_strs, 'envidas.csv', sep = '_')
  req_files = file.path(outdir, format(dates, '%Y'), req_file_names)
  ## existing files
  file_glob = file.path(outdir, '*', '*')
  cur_files = Sys.glob(file_glob)
  ## download files
  is_new = !req_file_names %in% basename(cur_files)
  new_dates = dates[is_new]
  new_files = req_files[is_new]
  out_folders = unique(dirname(new_files))
  if (any(is_new)) {
    for (f in out_folders) {
      dir.create(f, showWarnings = FALSE, recursive = TRUE)
    }
    for (n in 1:sum(is_new)) {
      d = dates[n]
      out_file = new_files[n]
      message('Downloading ', out_file)
      df_n = get_envidas(con, site, minutes, d, d + 1)
      write.csv(df_n, file = out_file, row.names = F)
    }
  }
}

#' @export
update_nysmesonet = function(outdir, nysm_api, nysm_site, start_date,
                             end_date = Sys.Date()) {
  d = seq(start_date, end_date - 1, 'day')
  date_str = format(d, '%Y%m%d')
  mes_url = paste0(nysm_api, '/', nysm_site, '/', date_str, 'T0000/',
                   date_str, 'T2355')
  file_name = paste(date_str, 'mesonet.csv', sep = '_')
  out_folder = format(d, '%Y')
  out_path = file.path(out_folder, file_name)
  ## create directories if needed
  full_out_folder_paths = unique(file.path(outdir, out_folder))
  for (f in full_out_folder_paths) {
    dir.create(f, showWarnings = FALSE, recursive = TRUE)
  }
  obj = list()
  attributes(obj)$raw_dir = outdir
  etl::smart_download(obj, mes_url, out_path)
}

list_remote_files = function(sshcon, path) {
  ls_command = paste('ls', path)
  ls_res = ssh_exec_internal(sshcon, ls_command)
  strsplit(rawToChar(ls_res$stdout), '\n')[[1]]
}

#' @export
update_campbell = function(outdir, sshcon, remote_path) {
  remote_files = list_remote_files(sshcon, remote_path)
  ## we only want the 'Table1' files
  remote_files = remote_files[grep('Table1', remote_files)]
  raw_campbell_glob = file.path(outdir, '*', '*')
  old_files = Sys.glob(raw_campbell_glob)
  new_files = remote_files[!remote_files %in% basename(old_files)]
  if (length(new_files) > 0) {
    new_file_years = substring(gsub('^.*Table1_', '', new_files), 1, 4)
    new_remote_paths = file.path(remote_path, new_files)
    new_local_paths = file.path(outdir, new_file_years)
    for (f in unique(new_local_paths)) {
      dir.create(f, showWarnings = FALSE, recursive = TRUE)
    }
    for (n in 1:length(new_remote_paths)) {
      ssh::scp_download(sshcon, new_remote_paths[n], to = new_local_paths[n])
   }
  }
}
