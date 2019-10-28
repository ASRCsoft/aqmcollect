## functions to update the raw data repository

#' @export
download_mesonet = function(obj, mesonet_api_url, d) {
  d = as.Date(d)
  date_str = format(d, '%Y%m%d')
  mes_url = paste0(mesonet_api_url, '/WFMB/',
                   date_str, 'T0000/',
                   date_str, 'T2355')
  file_name = paste(date_str, 'mesonet.csv', sep = '_')
  out_folder = file.path('WFML', 'measurements', 'mesonet', format(d, '%Y'))
  out_path = file.path(out_folder, file_name)
  ## create directories if needed
  full_out_folder_paths = unique(file.path(attr(obj, "raw_dir"), out_folder))
  for (f in full_out_folder_paths) {
    dir.create(f, showWarnings = FALSE, recursive = TRUE)
  }
  etl::smart_download(obj, mes_url, out_path)
}

list_remote_files = function(sshcon, path) {
  ls_command = paste('ls', path)
  ls_res = ssh_exec_internal(sshcon, ls_command)
  strsplit(rawToChar(ls_res$stdout), '\n')[[1]]
}

#' @export
update_campbell = function(obj, site, sshcon, remote_path) {
  remote_files = list_remote_files(sshcon, remote_path)
  ## we only want the 'Table1' files
  remote_files = remote_files[grep('Table1', remote_files)]
  raw_campbell_glob = file.path(attr(obj, 'raw_dir'), site, 'measurements', 'campbell', '*', '*')
  old_files = Sys.glob(raw_campbell_glob)
  new_files = remote_files[!remote_files %in% basename(old_files)]
  if (length(new_files) > 0) {
    new_file_years = substring(gsub('^.*Table1_', '', new_files), 1, 4)
    new_remote_paths = file.path(remote_path, new_files)
    new_local_paths = file.path(attr(obj, 'raw_dir'), site, 'measurements', 'campbell',
                                new_file_years)
    for (f in unique(new_local_paths)) {
      dir.create(f, showWarnings = FALSE, recursive = TRUE)
    }
    for (n in 1:length(new_remote_paths)) {
      ssh::scp_download(sshcon, new_remote_paths[n], to = new_local_paths[n])
    }
  }
}

## update_raw_data = function(obj, mesonet_api_url, campbell_ssh) {
##   ## get NYS Mesonet data from the first recorded data to yesterday
##   tnow = Sys.time()
##   attributes(tnow)$tzone = 'UTC'
##   dnow = as.Date(tnow) - 1
##   d = seq(as.Date('2016-01-29'), dnow, 'day')
##   message('Downloading NYS Mesonet files...')
##   download_mesonet(obj, mesonet_api_url, d)
##   message('Downloading WFMS Campbell files...')
##   update_campbell(obj, 'WFMS', campbell_ssh, '/RA1/loggernet/summit')
##   message('Downloading WFML Campbell files...')
##   update_campbell(obj, 'WFML', campbell_ssh, '/RA1/loggernet/lodge')
## }
