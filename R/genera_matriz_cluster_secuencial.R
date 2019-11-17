

# generamos la matriz total para clusterizar de forma secuencial, de 10.000 en 10.000 usuarios, controlando la memoria


library(dplyr)
library(ggplot2)
library(tidyr)
library(data.table)
library(lubridate)
rm(list = ls());gc()
source("funciones_tratamiento_datos.R")

system.time(datos_total <- fread('datos/DATATHON_secuencial.csv', header = T, sep = '|')) 

# Extraigo atributos de usuarios antes de organizar
setkey(datos_total, IDENTIFICADOR)
atr_usu <- unique(datos_total)[, .(IDENTIFICADOR, DE_MUNICIP, FECHA_ALTA_STRO,TARGET_TENENCIA_CUPS, CNAE, PRODUCTO, MERCADO, POT=NA), ]
atr_usu$POT <- as.numeric(atr_usu$POT)

ntotal_usuarios <- nrow(atr_usu)
block_size <- 10000
nvueltas <- round(ntotal_usuarios/block_size, 0)-1

FECHAI <- as.POSIXct("2014-10-01")
FECHAF <- as.POSIXct("2015-09-30 23:00")

missing_users <- c()
lowvar_users <- c()

# función que rellena registros missing con climatologia
myfun <- function(h,x){ x[is.na(x)] <- as.numeric(medios_horarios[hour==h,which(is.na(x))+1, with=FALSE]);return(x)}

fechas <- data.table(fecha=seq(FECHAI, FECHAF, '1 hour'))
setkey(fechas, fecha)

# Maxima proporcion de missings permitida
max_missing_rate=0.80
# Porcentaje de usuarios con varianza mas baja que desechamos 
min_var_thres=0.015

# Percentil que marca la potencia contratada
perc_pot_max=0.99

for (i in 0:nvueltas){
#for (i in 0:0){
    
    cat(i,'/', nvueltas, '\n', format(now(), '%F %T'), '\n')
    
    usuarios_block <- (1:block_size) + block_size* i
    datos <- OrganizaDatosDT(datos_total[IDENTIFICADOR %in% usuarios_block], wide=T, filtra_dupl = T)
    gc()
    
    activa <-dcast(datos[,.(IDENTIFICADOR, fecha, ACTIVA)], fecha~IDENTIFICADOR)
    
    #Control serie completa de fechas
    activa <- merge(fechas, activa, all.x=TRUE)
    
    columnas <- colnames(activa)
    
    # extraigo valor máximo por usuario y lo guardo en df de usuarios
    pot_max <- apply(activa[,-1, with=FALSE], 2, function(x) quantile(x, probs = perc_pot_max, na.rm=T))
    atr_usu[IDENTIFICADOR %in% columnas[-1], POT:=pot_max]
    
    # Eliminamos clientes sin datos distintos de cero
    #zero_users <- as.character(atr_usu[POT==0,IDENTIFICADOR])
    
    activa <- activa[,-(which(pot_max==0)+1), with=FALSE]
    columnas <- colnames(activa)
    
    pot_max2 <- pot_max[which(pot_max>0)]
    # Filtro valores por encima de la pot_max
    activa <- cbind(activa[,1,with=FALSE],
                    activa[, 
                           lapply(1:ncol(.SD),
                                  function(i) ifelse(.SD[,i,with=FALSE][[1]]>pot_max2[i], 
                                                     NA, 
                                                     .SD[,i,with=FALSE][[1]])), 
                           .SDcols=-1])
    
    colnames(activa) <- columnas
    
    # Filtramos series con muchos missings y con varianzas muy bajas
    aux <- HighMissingRateFilter(activa, max_missing_rate, colindex = 'fecha', devuelve_filtradas = T)
    activa <- aux$datos
    missing_users <- c(missing_users,aux$filtrados)
    #Filtro por baja varianza, el 2% a la calle
    aux <- LowVarianceFilter(datos = activa, colindex = 'fecha', var_thres = min_var_thres, devuelve_filtradas = T)
    activa <- aux$datos
    lowvar_users <- c(lowvar_users, aux$filtrados)
    rm(aux); gc()
    
    columnas <- colnames(activa)
    
    # Rellenamos missings con valores medios horarios
    medios_horarios <- activa[, lapply(.SD[-1], mean, na.rm=T), by=.(hour(fecha))]
    activa <- cbind(activa[,1,with=FALSE], t(apply(activa, 1, function(x) myfun(hour(x[1]), as.numeric(x[-1])))))
    
    # normalizo por valor máximo.
    #nactiva <- cbind(activa[,1, with=FALSE],apply(activa[,-1, with=FALSE], 2, function(x) x/quantile(x, probs = 0.999, na.rm=T)))
    nactiva <- cbind(activa[,1, with=FALSE],apply(activa[,-1, with=FALSE], 2, function(x) x/max(x, na.rm=T)))
    colnames(nactiva) <- columnas
    
    # if (i==0){
         matriz_cluster <- t(nactiva[,-1,with=FALSE])
    # }else{
    #     matriz_cluster <- rbind(matriz_cluster, t(nactiva[,-1,with=FALSE]))
    # }
    
    #guardamos en ficheros de texto
    status <- sapply(1:nrow(matriz_cluster), function(i){
        if (i %% 1000 == 0)
            cat(i, '\n')

        client_file <- sprintf('user_rowfiles/%s_filtered_nactiva.txt', rownames(matriz_cluster)[i]) 
        ff <- file(client_file)
        writeLines(
            as.character(paste0(round(matriz_cluster[i, ],6), collapse=" ")),
            con = ff)
        close(ff)
        subeBucket(client_file)
        # TODO rm ff upon successful upload to S3 bucket
    })
    
    rm(matriz_cluster)
    gc()
}

#ojo, transponerla dentro del bucle mejor que fuera de una vez

saveRDS(atr_usu, 'datos/salidas_web/usuarios.rds')
saveRDS(missing_users, sprintf('datos/salidas_web/missing_users%f.rds', max_missing_rate))
saveRDS(lowvar_users, sprintf('datos/salidas_web/lowvar_users%f.rds', min_var_thres))
#saveRDS(matriz_cluster, 'datos/matriz_total_cluster.rds')


