# Recopilo aquí funciones para manejar la info
# Quizá fuera bueno que terminara siendo un paquete.


# Constantes del proyecto
# Fechas del periodo usado para clusterizar
FECHAI <- as.POSIXct("2014-10-01")
FECHAF <- as.POSIXct("2015-09-30 23:00")



# Transforma los datos en formato ancho (tal como vienen en el csv) en formato largo horario
# rawdata clase data.table o data.frame sencillo?
# wide: si queremos una columna para activa y otra para reactiva, si FALSE, devuelve formato largo
# atr_usu: TRUE si queremos mantener los campos de atributos del usuario en el df de salida
OrganizaDatos <- function(rawdata, wide=FALSE, atr_usu=FALSE){
    require(tidyr)
    require(lubridate)
    
    cols <- colnames(rawdata)

    #datos energia
    rawdata %>%  dplyr::select(DIA,IDENTIFICADOR, grep('ACTIVA', cols)) %>%  
        gather(var, consumo, -DIA,-IDENTIFICADOR) %>% 
        separate(var, c('variable', 'hora')) %>% 
        filter(hora!="H25") %>% 
        mutate(DIA = as.Date(as.character(DIA),format="%Y%m%d",tz="Europe/Madrid"),
               hora= as.integer(gsub("H","",hora)),
               variable=as.factor(variable),
               IDENTIFICADOR = as.factor(IDENTIFICADOR),
               fecha=as.POSIXct(DIA+hours(hora-1))) %>% 
        dplyr::select(-DIA,-hora) %>% 
        arrange(IDENTIFICADOR,variable,fecha) -> energia
    
    # incluimos atributos de usuario
    if (atr_usu){
        rawdata %>% distinct(IDENTIFICADOR) %>% 
            dplyr::select(IDENTIFICADOR, DE_MUNICIP, FECHA_ALTA_STRO,TARGET_TENENCIA_CUPS, CNAE, PRODUCTO, MERCADO) %>%
            mutate(IDENTIFICADOR=as.factor(IDENTIFICADOR))-> atr_usu
        
        energia <- left_join(energia, atr_usu, by='IDENTIFICADOR')
    }
    
    if (wide){
        energia <- energia %>% spread(variable, consumo)
    }
        
    
    return(energia)
}


#igual que la anterior pero usando data.table
OrganizaDatosDT <- function(rawdata, wide=FALSE, atr_usu=FALSE, filtra_dupl=F){
    require(data.table)
    require(lubridate)
    
    setkey(rawdata, DIA, IDENTIFICADOR)
    
    if (filtra_dupl)
        rawdata <- FiltraDuplIDDIA(rawdata, T)
    
    energia <- melt(rawdata[, grep('DIA|IDENTIFICADOR|ACTIVA', colnames(rawdata)), with=FALSE], measure=patterns("^ACTIVA","^REACTIVA"),
                    value.name =c("ACTIVA", "REACTIVA"))[variable != 25,]
    colnames(energia) <- c("DIA", "IDENTIFICADOR", "HORA", "ACTIVA", "REACTIVA")
    
    energia <- energia[,.(IDENTIFICADOR,
                          fecha=as.POSIXct(as.Date(as.character(DIA),format="%Y%m%d",tz="Europe/Madrid") + hours(as.integer(HORA)-1)),
                          ACTIVA, REACTIVA)]
    
    if(atr_usu){
        setkey(rawdata, IDENTIFICADOR)
        atr_usu <- unique(rawdata)[, .(IDENTIFICADOR, DE_MUNICIP, FECHA_ALTA_STRO,TARGET_TENENCIA_CUPS, CNAE, PRODUCTO, MERCADO), ]
        setkey(atr_usu, IDENTIFICADOR)
        setkey(energia, IDENTIFICADOR, fecha)
        energia <- energia[atr_usu, nomatch=0]
    }
    
    if (!wide){
        energia <- melt(energia, measure.vars = c("ACTIVA", "REACTIVA"), variable.name = "variable", value.name = "consumo")
    }

    
    return(energia)
}

# Wrapper para subir ficheros al Bucket de AWS
subeBucket <- function(file, host='s3-eu-central-1.amazonaws.com', bucket='mdcendesa'){
  require(S3)
  # link access info to the corresponding vars
  S3_connect(access_key = MYACCESSKEY, secret_key = MYKEY,hostname = host)
  st <- S3_put_object(bucket, file)
  
}
# Funciones de filtrado de datos
#'
#'@description Funcion de filtrado de registros duplicados para la dupla IDENTIFICADOR-DIA. En primera versión sólo deja el
#' primer registro duplicado que encuentra. Es más rápido
#'
#'@param datos: data.frame o data.table con datos
#'@param rawformat: TRUE si vienen en formato original del API
#'
FiltraDuplIDDIA <- function(datos, rawformat=F){
    
    # to do: transformar datos a data.table si no lo es
    
    if (rawformat){
        setkey(datos, IDENTIFICADOR, DIA)
        return(unique(datos))    
    }else{
        setkey(datos, IDENTIFICADOR, fecha)
        return(unique(datos))
    }
}

#' @description Función que elimina de un dataset las variables con un % de missings dado. En principio pensado para filtrar usuarios
#' con un numero minimo de observaciones, pero se puede generalizar
#' @param datos: dataset (data.table) cuyas columnas hay que filtrar. Ha de estar procesado, es decir, tener una columna de fechas
#' completa y una columna por usuario
#' @param missing_rate: proporcion máxima permitida de missings
#' @param colindex: columna indice (fecha)
#' @param devuelve_filtradas: devuelve una lista con la serie filtrada y un array con los nombres de columnas filtrados
#' @param solo_filtradas: devuelve solamente las series que filtraría del dataset de entrada. Dry run.
#' 
HighMissingRateFilter <- function(datos, missing_rate=0.5, colindex='fecha', devuelve_filtradas=FALSE, solo_filtradas=FALSE){
    # to do: transformar datos a data.table si no lo es
  
   propna <- PropPresent(datos, colindex)
   if (solo_filtradas){
       return(names(propna[which(propna<=1-missing_rate)]))
   }
   
   
   if (devuelve_filtradas){
       return(list(datos=datos[, which(propna>1-missing_rate), with=FALSE], 
                   filtrados=names(propna[which(propna<=1-missing_rate)])))
       
   }else{
        return(datos[, which(propna>1-missing_rate), with=FALSE])
       
   }
}

# Devuelve porcentajes de datos disponibles por columna suponiendo que la columna indice va completa
PropPresent <- function(datos, colindex='fecha'){
    nna <- apply(datos, 2, function(x) sum(!is.na(x)))
    return(nna/nna[colindex])
}


#' @description Función que elimina de un dataset las variables con varianza por debajo de un umbral dado, tras normalizar.
#' Sirve para eliminar columnas con muchos valores iguales (ceros por ejemplo) y otras cosas raras.
#' @param datos: dataset (data.table) cuyas columnas hay que filtrar. Ha de estar procesado, es decir, tener una columna de fechas
#' completa y una columna por usuario
#' @param colindex: nombre o indice de la columna índice (por defecto 'fecha')
#' @param var_thres: valor del percentil de las varianzas de las columnas por debajo del cual filtramos columnas. Handle with care.
#' @param devuelve_filtradas: devuelve una lista con la serie filtrada y un array con los nombres de columnas filtrados
#' @param solo_filtradas: devuelve solamente las series que filtraría del dataset de entrada. Dry run.


LowVarianceFilter <- function(datos, colindex='fecha', var_thres=0.01, devuelve_filtradas=FALSE, solo_filtradas=FALSE){
    #normalizo por el valor máximo de cada serie
    datosnorm <- apply(datos[,-colindex, with=FALSE], 2, function(x) x/max(x, na.rm=T))
    
    varianzas <- apply(datosnorm,2, function(x) (sd(x, na.rm=T))^2)
    
    # si varianza = NA -> todos los valores iguales a cero
    # si varianza = 0 -> todos los valores iguales. 
    varlim <- quantile(varianzas, var_thres, na.rm=T)
    
    if (solo_filtradas){
        return(names(varianzas[varianzas<varlim | is.na(varianzas)]))    
    }
    
    if (devuelve_filtradas){
        return(list(datos=datos[, c(colindex, names(varianzas[varianzas>=varlim & !is.na(varianzas)])), with=FALSE],
                    filtrados=names(varianzas[varianzas<varlim | is.na(varianzas)])))
    }else{
        
        return(datos[, c(colindex, names(varianzas>=varlim & !is.na(varianzas))), with=FALSE])
    }
    
}

#' 
#' @description Funcion generica para pintar un dygraph a partir de un data.frame
#' 
#' @param datos: Datos a pintar. Si no se especifica la columna fecha trata de adivinarla por el tipo (is.POSIXct || is.Date)
#' @param Identificador de la columna fecha
#' @param ... Argumentos para la funcion dygraph
#' 

dygraph_df<-function(datos,columna_fecha=NULL, ...){
    library(dygraphs)
    library(xts)
    library(lubridate)
    
    if (is.null(columna_fecha)){
        columna_fecha = which(sapply(datos,function(col){is.POSIXct(col) | is.Date(col) }))
    }
    if (length(columna_fecha) != 1){
        stop("Hay mas de una columna fecha o ninguna: ",columna_fecha)
    }
    nombre_columna_fecha<-colnames(datos)[columna_fecha]
    dygraph(xts(dplyr::select_(datos,paste0("-",nombre_columna_fecha)),
                datos[[columna_fecha]]),...) %>% dyRangeSelector()
}

#' 
#' @description Esta funcion puede ser util cuando tengamos los clusters para visualizar las series
#' 
#' @param datos: datos en formato rawdata: leidos directamente del csv
#' @param usuarios: vector de enteros de IDENTIFICADOR de usuarios para mostrar. Si este vector es muy tocho puede 
#' que te tumbe la maquina.
#' 
PintarSeriesUsuarios<-function(datos,usuarios){
    require(dygraphs)
    require(xts)
    
    datos_usuarios = datos %>% filter(IDENTIFICADOR %in% usuarios[1:5])
    datos_usuarios_largo = OrganizaDatos(datos_usuarios)
    datos_usuarios_largo$ID_variable = paste(datos_usuarios_largo$IDENTIFICADOR,datos_usuarios_largo$variable,sep="_")
    duplicados = duplicated(dplyr::select(datos_usuarios_largo,ID_variable,fecha))
    if (any(duplicados)){
        cat("Hay datos duplicados: nos los fumigamos")
        #print(datos_usuarios_largo[duplicados,])
        datos_usuarios_largo = datos_usuarios_largo[!duplicados,]
    }
    
    datos_usuarios_largo %>% 
        dplyr::select(fecha,ID_variable,consumo) %>% 
        spread(ID_variable,consumo)->datos_ancho_pintar_dygraph %>% 
        dygraph_df()
}


# Funciones de lectura de las salidas del clustering de spark

LeeCentroidesClusters <- function(fileout='../spark/output/centroides_kmeans.csv', ini_date=FECHAI, end_date=FECHAF){
    dataset <- read.csv(fileout, header=F)
    colnames(dataset) <- paste('cl', 1:ncol(dataset), sep='_')
    return(data.frame(fecha=seq(FECHAI, FECHAF, '1 hour'), dataset))
}

LeeAsignacionClusters <- function(fileout='../spark/output/asignacion_kmeans.csv'){
    dataset <- read.csv(fileout, header=F, sep=';')
    colnames(dataset) <- c('IDENTIFICADOR', "cluster")
    return(dataset %>% mutate(cluster=cluster+1))
}

