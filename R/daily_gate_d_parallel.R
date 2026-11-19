gate_p_parent <- function() "daehoon/data/daily_xgb_gate_d/year=2023/month=01/run=all593_optimized_001_continuation_v2"
gate_p_config <- function() list(version="parallel_v3",workers=8L,nthread=5L,blas=1L,
  aggregate_warning_mib=81920,aggregate_hard_mib=98304,worker_guard=gate_d_v2_config(),paused_pid=3776687L)
gate_p_memory_status <- function(rss) {
  if(length(rss)!=1L||!is.finite(rss)||rss<0)stop("Invalid aggregate RSS.")
  list(warning=rss>gate_p_config()$aggregate_warning_mib,stop=rss>gate_p_config()$aggregate_hard_mib)
}
gate_p_paused <- function() {
  s<-readLines(paste0("/proc/",gate_p_config()$paused_pid,"/status"))
  if(!grepl("^State:.*T",grep("^State:",s,value=TRUE)))stop("Sequential parent is not paused.")
  invisible(TRUE)
}
gate_p_index <- function(root) {
  cached<-getOption("gate_p_index")
  if(is.null(cached)||!identical(attr(cached,"root"),root)) {
    cached<-read.csv(file.path(root,"partition_index.csv"));attr(cached,"root")<-root
    options(gate_p_index=cached)
  }
  cached
}
gate_p_entry <- function(root,i,date) {
  gate_c_assert_date(as.Date(date));j<-(i-1L)*31L+match(as.Date(date),january_smoke_dates())
  e<-gate_p_index(root)[j,]
  if(nrow(e)!=1||e$position!=i||e$date!=as.character(date))stop("Partition index mismatch.")
  e
}
gate_p_manifest <- function(m,row,date) {
  needed<-c("status","contract_version","chunk_id","annual_branch","contract_branch","date","rows","fields","schema","models",
    "static_normalization","predictor_missing","static_missing_rows","timing","sha256","finite_counts","spatial_sha256","identity_preserved",
    "source_snapshot","source_snapshot_sha256","metrics","output_bytes","origin")
  if(!all(needed %in% names(m)))stop("Incomplete parallel manifest.")
  if(m$status!="PASS"||m$contract_version!="parallel_v3"||m$origin!="predicted_gate_d_parallel_v3"||m$chunk_id!=row$chunk_id||
    m$annual_branch!=row$annual_branch||m$contract_branch!=row$contract_branch||m$date!=as.character(date)||m$rows!=row$cells||
    !identical(unlist(m$fields,use.names=FALSE),gate_d_fields())||!identical(m$schema,daily_model_schema())||!isTRUE(m$identity_preserved))stop("Parallel identity/schema mismatch.")
  if(!identical(names(m$models),c("PM10","PM25"))||!identical(names(m$timing),c("PM10","PM25")))stop("Pollutant identity mismatch.")
  for(p in c("PM10","PM25")) {
    t<-m$timing[[p]]
    if(m$models[[p]]$sha256!=gate_d_hashes()[[p]]||m$finite_counts[[p]]!=row$cells||t$blas_before!=1||t$blas_after!=1||t$xgb_nthread!=5||
      !is.finite(t$negative)||t$negative<0||t$negative>row$cells)stop("Parallel model/thread/finite mismatch.")
  }
  a<-m$static_normalization
  if(a$chunk_id!=row$chunk_id||a$annual_branch!=row$annual_branch||a$nan_converted!=row$static_missing||a$normalized_nan!=0||a$positive_inf!=0||a$negative_inf!=0||
    !isTRUE(a$finite_values_unchanged)||!isTRUE(a$metadata_unchanged))stop("Normalization mismatch.")
  if(!identical(names(m$predictor_missing),daily_predictor_terms())||any(!is.finite(unlist(m$predictor_missing)))||
    any(unlist(m$predictor_missing)<0|unlist(m$predictor_missing)>row$cells))stop("Missingness counts invalid.")
  invisible(TRUE)
}
gate_p_partition <- function(root,row,date,spatial=NULL) {
  e<-gate_p_entry(root,row$position,date);path<-e$path
  if(e$nthread==40L)return(gate_d_v2_partition(path,row,as.Date(date),spatial))
  if(e$nthread!=5L||!file.exists(path)||!file.exists(paste0(path,".json"))||any(file.exists(c(paste0(path,".partial"),paste0(path,".json.partial")))))stop("Missing/partial parallel partition.")
  m<-jsonlite::read_json(paste0(path,".json"),simplifyVector=TRUE);gate_p_manifest(m,row,date)
  if(m$sha256!=digest::digest(file=path,algo="sha256")||m$output_bytes!=file.info(path)$size)stop("Parallel Parquet checksum/size mismatch.")
  if(!file.exists(m$source_snapshot)||digest::digest(file=m$source_snapshot,algo="sha256")!=m$source_snapshot_sha256)stop("Source snapshot checksum mismatch.")
  data<-as.data.frame(nanoparquet::read_parquet(path))
  if(!identical(names(data),gate_d_fields())||nrow(data)!=row$cells||!inherits(data$date,"Date")||!all(data$date==as.Date(date))||
    !all(data$chunk_id==row$chunk_id)||anyNA(data[gate_d_spatial_fields()])||anyDuplicated(data$gid)||anyDuplicated(data$grid_cell_id)||
    any(!is.finite(data$PM10))||any(!is.finite(data$PM25)))stop("Parallel stored identity/finite mismatch.")
  canonical<-gate_d_identity_digest(data[gate_d_spatial_fields()])
  if(canonical!=m$spatial_sha256)stop("Canonical spatial digest differs.")
  if(!is.null(spatial))gate_d_identity_equal(data[gate_d_spatial_fields()],spatial)
  for(p in c("PM10","PM25"))if(sum(data[[p]]<0)!=m$timing[[p]]$negative)stop("Negative count differs.")
  list(data=data,manifest=m,canonical_spatial=canonical)
}
gate_p_checks <- function(root,row,spatial=NULL) {
  checks<-lapply(as.character(january_smoke_dates()),function(date){
    x<-gate_p_partition(root,row,date,spatial);e<-gate_p_entry(root,row$position,date);m<-x$manifest
    data.frame(date=date,rows=nrow(x$data),sha256=m$sha256,spatial_sha256=x$canonical_spatial,bytes=m$output_bytes,
      origin=e$origin,nthread=e$nthread,path=e$path,manifest_sha256=digest::digest(file=paste0(e$path,".json"),algo="sha256"))
  })
  d<-do.call(rbind,checks)
  if(length(unique(d$spatial_sha256))!=1||sum(d$rows)!=row$cells*31)stop("Chunk month identity mismatch.")
  d
}
gate_p_check_chunk <- function(root,row,spatial=NULL) {
  checks<-gate_p_checks(root,row,spatial)
  done<-jsonlite::read_json(file.path(root,"chunk_manifests",paste0(row$class,".json")),simplifyVector=TRUE)
  if(done$status!="PASS"||done$chunk_id!=row$chunk_id||done$annual_branch!=row$annual_branch||done$rows!=row$cells*31||
    !identical(done$partitions$sha256,checks$sha256)||!identical(done$partitions$manifest_sha256,checks$manifest_sha256))stop("Parallel chunk completion mismatch.")
  s<-read.csv(file.path(root,"inputs",paste0(row$class,".csv")))
  if(!identical(s$sha256,gate_b_snapshot(s$path)$sha256))stop("Source artifact changed.")
  list(checks=checks,done=done)
}
gate_p_signal <- function(root,reason) {
  dir.create(file.path(root,"STOP"),showWarnings=FALSE)
  path<-file.path(root,"logs",paste0("failure_",Sys.getpid(),".json"))
  if(!file.exists(path))write_daily_validation_json(list(status="BLOCKED",reason=reason),path)
}
gate_p_check_stop <- function(root) {
  if(dir.exists(file.path(root,"STOP")))stop("Parallel STOP gate is set.")
  gate_p_paused();invisible(TRUE)
}
gate_p_predict <- function(batch,prepared,date,models) {
  gate_c_assert_batch(batch,prepared,date);out<-batch[gate_d_fields()[1:6]];x<-batch[daily_model_predictor_terms()];timing<-list()
  for(p in c("PM10","PM25")) {
    cpu_bench_threads(5L);engine<-workflows::extract_fit_engine(models[[p]]$workflow)
    stopifnot(xgboost::xgb.config(engine)$learner$generic_param$nthread=="5")
    t<-proc.time()["elapsed"];out[[p]]<-predict(models[[p]]$workflow,new_data=x)$.pred
    sec<-unname(proc.time()["elapsed"]-t);cpu_bench_threads(5L)
    if(xgboost::xgb.config(engine)$learner$generic_param$nthread!="5"||length(out[[p]])!=nrow(batch)||any(!is.finite(out[[p]])))stop("Prediction/thread contract failed.")
    timing[[p]]<-list(seconds=sec,blas_before=1L,blas_after=1L,xgb_nthread=5L,negative=sum(out[[p]]<0))
  }
  gate_c_equal(out[gate_d_fields()[1:6]],batch[gate_d_fields()[1:6]])
  list(data=out,timing=timing)
}
gate_p_write <- function(root,prepared,models,date,snapshot) {
  row<-prepared$row;e<-gate_p_entry(root,row$position,date);spatial<-prepared$blueprint[gate_d_spatial_fields()]
  if(e$nthread==40L){x<-gate_p_partition(root,row,date,spatial);return(as.data.frame(x$manifest$metrics))}
  path<-e$path
  if(any(file.exists(c(path,paste0(path,".json"),paste0(path,".partial")))))stop("Refusing parallel output overwrite.")
  gate_p_check_stop(root);begin<-proc.time();t<-proc.time()["elapsed"]
  dynamic<-materialize_gate_c_date(prepared,date);material_seconds<-unname(proc.time()["elapsed"]-t)
  t<-proc.time()["elapsed"];batch<-attach_gate_c_static(dynamic$data,prepared,date);attach_seconds<-unname(proc.time()["elapsed"]-t)
  pred<-gate_p_predict(batch,prepared,date,models);miss<-vapply(batch[daily_predictor_terms()],function(x)sum(is.na(x)),integer(1))
  era<-sum(rowSums(is.na(batch[c("t2m","u10","v10","sp","ssr","tp")]))>0);anydynamic<-sum(rowSums(is.na(batch[daily_predictor_terms()]))>0)
  gate_d_identity_equal(pred$data[gate_d_spatial_fields()],spatial)
  t<-proc.time()["elapsed"];nanoparquet::write_parquet(pred$data,paste0(path,".partial"))
  if(!file.rename(paste0(path,".partial"),path))stop("Parquet install failed.")
  serialization<-unname(proc.time()["elapsed"]-t);a<-prepared$audit;a$per_predictor<-as.list(a$per_predictor)
  elapsed<-proc.time()-begin
  metrics<-data.frame(position=row$position,chunk_id=row$chunk_id,date=as.character(date),n_rows=nrow(batch),materialization_seconds=material_seconds,
    static_attach_seconds=attach_seconds,pm10_seconds=pred$timing$PM10$seconds,pm25_seconds=pred$timing$PM25$seconds,serialization_seconds=serialization,
    total_seconds=unname(elapsed["elapsed"]),cpu_seconds=sum(elapsed[c("user.self","sys.self")]),rss_during_batch_mib=gate_b_rss()[[1]],hwm_mib=gate_b_rss()[[2]],
    output_bytes=file.info(path)$size,era5_missing=era,any_dynamic_missing=anydynamic,static_missing=prepared$static_missing)
  manifest<-list(status="PASS",contract_version="parallel_v3",origin="predicted_gate_d_parallel_v3",chunk_id=row$chunk_id,annual_branch=row$annual_branch,
    contract_branch=row$contract_branch,date=as.character(date),rows=nrow(batch),fields=gate_d_fields(),schema=prepared$schema,models=lapply(models,`[[`,"source"),
    static_normalization=a,predictor_missing=as.list(miss),static_missing_rows=prepared$static_missing,timing=pred$timing,sha256=digest::digest(file=path,algo="sha256"),
    finite_counts=list(PM10=nrow(batch),PM25=nrow(batch)),spatial_sha256=gate_d_identity_digest(spatial),identity_preserved=TRUE,source_snapshot=snapshot,
    source_snapshot_sha256=digest::digest(file=snapshot,algo="sha256"),metrics=as.list(metrics),output_bytes=file.info(path)$size,any_era5_missing=era,any_dynamic_missing=anydynamic)
  write_daily_validation_json(manifest,paste0(path,".json"));x<-gate_p_partition(root,row,date,spatial);gate_c_equal(x$data,pred$data)
  rm(batch,dynamic,pred,x);gc();metrics$rss_after_gc_mib<-gate_b_rss()[[1]];gate_d_memory_guard(numeric())
  metrics
}
gate_p_worker_chunk <- function(i) {
  root<-P_ROOT;row<-P_REGISTRY[i,];gate_p_check_stop(root);begin<-proc.time();pre<-gate_b_rss()[[1]]
  directory<-file.path(root,"partitions",row$class)
  if(dir.exists(directory))stop("New chunk directory exists; explicit recovery required.")
  dir.create(directory)
  contract<-P_CATALOG$read(row$contract_branch)
  paths<-c(file.path("_targets/objects",unlist(row[c("annual_branch","grid_branch","contract_branch")])),contract$cell_map_file,unlist(lapply(contract$sources,`[[`,"cube_files")))
  snapshot<-gate_b_snapshot(paths);snapshot_path<-file.path(root,"inputs",paste0(row$class,".csv"))
  if(file.exists(snapshot_path)) {
    if(!identical(read.csv(snapshot_path)$sha256,snapshot$sha256))stop("Inherited partial source drift.")
  }else gate_d_atomic_csv(snapshot,snapshot_path)
  expected<-P_EXPECTED[match(row$chunk_id,P_EXPECTED$chunk_id),]
  if(snapshot$sha256[match(file.path("_targets/objects",row$annual_branch),snapshot$path)]!=expected$annual_file_sha256_before)stop("Annual source drift.")
  annual<-as.data.frame(P_CATALOG$read(row$annual_branch));grid<-as.data.frame(sf::st_drop_geometry(P_CATALOG$read(row$grid_branch)))
  prepared<-prepare_daily_static_chunk_for_prediction(annual,grid,row,contract,daily_canonical_template());a<-prepared$audit;a$per_predictor<-as.list(a$per_predictor)
  gate_d_normalization_check(list(a),P_EXPECTED);rm(annual,grid,contract);gc()
  prep_seconds<-unname((proc.time()-begin)["elapsed"]);records<-list()
  for(date in as.character(january_smoke_dates())) {
    gate_p_check_stop(root);records[[date]]<-gate_p_write(root,prepared,P_MODELS,as.Date(date),snapshot_path)
    active<-records[vapply(names(records),function(d)gate_p_entry(root,i,d)$nthread==5L,logical(1))]
    gate_c_memory_check(active)
  }
  checks<-gate_p_checks(root,row,prepared$blueprint[gate_d_spatial_fields()])
  if(!identical(snapshot$sha256,gate_b_snapshot(paths)$sha256))stop("Chunk source changed.")
  common<-Reduce(intersect,lapply(records,names));metrics<-do.call(rbind,lapply(records,function(x)x[common]))
  gate_d_atomic_csv(metrics,file.path(root,"logs",paste0(row$class,"_metrics.csv")))
  rm(prepared,records,active);gc();rss<-gate_b_rss();elapsed<-proc.time()-begin
  observation<-data.frame(session=paste0("worker_",Sys.getpid()),position=i,chunk_id=row$chunk_id,phase=gate_d_phase(i),cells=row$cells,
    pre_chunk_mib=pre,peak_mib=rss[[2]],post_gc_mib=rss[[1]])
  decision<-gate_d_v2_memory(P_HISTORY,observation);assign("P_HISTORY",rbind(P_HISTORY,observation),.GlobalEnv)
  gate_d_atomic_csv(decision,file.path(root,"logs",paste0(row$class,"_memory.csv")))
  summary<-data.frame(position=i,chunk_id=row$chunk_id,phase=gate_d_phase(i),pid=Sys.getpid(),rows=row$cells*31,
    inherited=sum(checks$nthread==40),predicted=sum(checks$nthread==5),wall_seconds=unname(elapsed["elapsed"]),cpu_seconds=sum(elapsed[c("user.self","sys.self")]),
    prep_seconds=prep_seconds,rss_mib=rss[[1]],hwm_mib=rss[[2]],bytes=sum(checks$bytes))
  write_daily_validation_json(list(status="PASS",chunk_id=row$chunk_id,annual_branch=row$annual_branch,rows=row$cells*31,normalization=a,summary=summary,partitions=checks),
    file.path(root,"chunk_manifests",paste0(row$class,".json")))
  if(decision$decision=="STOP")stop(decision$reason)
  gate_p_check_stop(root)
  write_daily_validation_json(list(status="PASS",position=i,rows=row$cells*31,rss_mib=rss[[1]],bytes=sum(checks$bytes)),file.path(root,"progress",paste0(row$class,".json")))
  cat("DONE",i,"RSS",rss[[1]],"guard",decision$trend_status,"\n")
  summary
}
gate_p_worker_bin <- function(positions) {
  tryCatch(lapply(positions,gate_p_worker_chunk),error=function(e){gate_p_signal(P_ROOT,conditionMessage(e));stop(e)})
}
gate_p_monitor <- function(root,pids,master_pid) {
  hz<-as.numeric(system2("getconf","CLK_TCK",stdout=TRUE));history<-list();last<-NULL
  repeat {
    now<-as.numeric(Sys.time());cpu<-rss<-0
    for(pid in unique(c(pids,master_pid,gate_p_config()$paused_pid))) {
      s<-strsplit(sub("^.*\\) ","",readLines(paste0("/proc/",pid,"/stat")))," +")[[1]]
      cpu<-cpu+(as.numeric(s[12])+as.numeric(s[13]))/hz
      m<-readLines(paste0("/proc/",pid,"/status"));rss<-rss+as.numeric(gsub("[^0-9]","",m[grepl("^VmRSS:",m)]))/1024
    }
    files<-list.files(file.path(root,"chunk_manifests"),pattern="json$",full.names=TRUE)
    n<-length(files)
    memory<-gate_p_memory_status(rss)
    if(memory$stop)gate_p_signal(root,paste("Aggregate RSS exceeds 96 GiB:",rss))
    if(length(history)%%15==0) {
      gate_p_paused();gate_d_disk_guard(root)
    }
    history[[length(history)+1L]]<-data.frame(time=now,rss_mib=rss,cpu_seconds=cpu,
      cpu_percent=if(is.null(last))NA_real_ else 100*(cpu-last$cpu)/(now-last$time),complete_chunks=n,
      warning=memory$warning,stop=dir.exists(file.path(root,"STOP")))
    if(length(history)%%15==0)gate_d_atomic_csv(tail(history,1)[[1]],file.path(root,"progress",sprintf("resource_%06d.csv",length(history))))
    last<-list(time=now,cpu=cpu)
    if(dir.exists(file.path(root,"MONITOR_FINISH")))break
    Sys.sleep(2)
  }
  gate_d_atomic_csv(do.call(rbind,history),file.path(root,"resources.csv"));TRUE
}
