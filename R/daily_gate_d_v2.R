gate_d_parent <- function() "daehoon/data/daily_xgb_gate_d/year=2023/month=01/run=all593_optimized_001"
gate_d_v2_config <- function() list(version="phase_size_v2",warmup=3L,baseline_n=3L,
  recent_n=9L,size_width=50000L,growth_mib=2048,slope_mib=16,
  increases=6L,warning_mib=12288,hard_mib=16384)

gate_d_v2_memory <- function(history, observation, config=gate_d_v2_config()) {
  o<-observation; rss<-o$post_gc_mib
  if(!is.finite(rss)||rss<0||!o$phase %in% c("REUSE","D1","D2","D3","D4"))stop("Invalid memory observation.")
  h<-rbind(history,o);phase<-h[h$phase==o$phase & h$session==o$session,]
  classes<-ceiling(phase$cells/config$size_width)
  high<-cummax(classes); regime<-high==tail(high,1)&classes==tail(classes,1)
  comparable<-phase[regime,];n<-nrow(comparable)
  post<-comparable$post_gc_mib
  eligible<-if(n>config$warmup)post[-seq_len(config$warmup)] else numeric()
  baseline<-growth<-slope<-NA_real_;increases<-0L;trend<-FALSE
  if(length(eligible)>=config$baseline_n+config$recent_n) {
    baseline<-mean(head(eligible,config$baseline_n)); recent<-tail(eligible,config$recent_n)
    growth<-mean(tail(recent,3))-baseline
    slope<-unname(coef(lm(recent~seq_along(recent)))[2])
    increases<-sum(diff(recent)>1)
    trend<-growth>config$growth_mib && slope>config$slope_mib && increases>=config$increases
  }
  hard<-rss>config$hard_mib
  cbind(o,data.frame(guard_version=config$version,size_class=tail(classes,1),highwater_size_class=tail(high,1),
    comparable_n=n,warmup=n<=config$warmup,window_positions=paste(comparable$position,collapse=","),
    baseline_mib=baseline,growth_mib=growth,slope_mib=slope,increases=increases,
    hard_status=if(hard)"FAIL" else "PASS",warning=rss>config$warning_mib,
    trend_status=if(trend)"FAIL" else if(is.na(baseline))"INSUFFICIENT_COMPARABLE_HISTORY" else "PASS",
    decision=if(hard||trend)"STOP" else "PASS",
    reason=if(hard)"Post-GC RSS exceeds 16384 MiB" else if(trend)
      sprintf("Comparable phase/size growth %.1f MiB; slope %.1f; increases %d",growth,slope,increases) else "Within absolute/comparable-trend limits"))
}

gate_d_identity_digest <- function(data,fields=gate_d_spatial_fields()) {
  if(!identical(names(data),fields))stop("Canonical identity column order differs.")
  # Named column lists avoid irrelevant data-frame attribute/row-name order.
  digest::digest(setNames(lapply(fields,function(n)data[[n]]),fields),algo="sha256")
}
gate_d_identity_equal <- function(actual,expected,fields=gate_d_spatial_fields()) {
  if(!identical(names(actual),fields)||!identical(names(expected),fields))stop("Identity column order differs.")
  gate_c_equal(actual,expected)
  if(gate_d_identity_digest(actual,fields)!=gate_d_identity_digest(expected,fields))stop("Canonical identity differs.")
  invisible(TRUE)
}
gate_d_registered_spatial <- function(row,catalog,template) {
  grid<-as.data.frame(sf::st_drop_geometry(catalog$read(row$grid_branch)))
  contract<-catalog$read(row$contract_branch);map<-read_grid_daily_cell_map(contract$cell_map_file)
  idx<-match(map$gid,grid$gid)
  if(anyNA(idx)||anyDuplicated(idx)||nrow(grid)!=row$cells||length(idx)!=row$cells)stop("Registered grid mismatch.")
  data.frame(chunk_id=row$chunk_id,gid=map$gid,grid_cell_id=daily_global_cell_id(grid[idx,],template),x=grid$x[idx],y=grid$y[idx])
}
gate_d_v2_partition <- function(path,row,date,spatial=NULL) {
  x<-gate_d_validate_partition(path,row,date)
  if(is.null(x))stop("Required partition missing: ",path)
  if(!is.null(spatial))gate_d_identity_equal(x$data[gate_d_spatial_fields()],spatial)
  x$canonical_spatial<-gate_d_identity_digest(x$data[gate_d_spatial_fields()]);x
}
gate_d_v2_owner <- function(root,i) if(i<=15L)gate_d_parent() else root
gate_d_v2_check_chunk <- function(root,row,spatial=NULL) {
  owner<-gate_d_v2_owner(root,row$position)
  dir<-file.path(owner,"partitions",row$class)
  if(length(list.files(dir,pattern="[.]partial$",recursive=TRUE)))stop("Orphan partial file: ",dir)
  checks<-gate_d_chunk_check(dir,row,spatial)
  if(!is.null(spatial)) {
    first<-gate_d_v2_partition(file.path(dir,"2023-01-01.parquet"),row,as.Date("2023-01-01"),spatial)
    if(first$canonical_spatial!=gate_d_identity_digest(spatial))stop("Canonical registered identity differs.")
  }
  done<-jsonlite::read_json(file.path(owner,"chunk_manifests",paste0(row$class,".json")),simplifyVector=TRUE)
  if(done$status!="PASS"||done$chunk_id!=row$chunk_id||done$annual_branch!=row$annual_branch||done$rows!=row$cells*31||
     !identical(done$partitions$sha256,checks$sha256)||!identical(done$partitions$manifest_sha256,checks$manifest_sha256))stop("Chunk completion differs.")
  snapshot<-file.path(owner,"inputs",paste0(row$class,".csv"))
  if(digest::digest(file=snapshot,algo="sha256")!=done$source_snapshot_sha256)stop("Source snapshot changed.")
  s<-read.csv(snapshot)
  if(!identical(s$sha256,gate_b_snapshot(s$path)$sha256))stop("Source file SHA256 changed.")
  checks$path<-file.path(dir,paste0(checks$date,".parquet"));checks$position<-row$position
  list(checks=checks,done=done)
}
