# Recopilo aquí funciones para manejar la info
# Quizá fuera bueno que terminara siendo un paquete.


# Constantes del proyecto
# Fechas del periodo usado para clusterizar
FECHAI <- as.POSIXct("2014-10-01")
FECHAF <- as.POSIXct("2015-09-30 23:00")


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

series <- LeeCentroidesClusters(fileout='output/centroides_kmeans.csv')
asignacion <- LeeAsignacionClusters('output/asignacion_kmeans.csv')

fecha_i <- as.POSIXct("2014-11-01")

usuarios <- as.data.frame(readRDS('output/usuarios.rds'))
usuarios <- left_join(usuarios, asignacion, by='IDENTIFICADOR')

head(usuarios)
#clases de potencia
usuarios <- usuarios  %>% mutate(power_class=cut(POT, breaks=quantile(POT, na.rm=T), include.lowest=T))
#clases de target_tenencia_cups
usuarios <- usuarios  %>% mutate(CUPS_class=cut(TARGET_TENENCIA_CUPS, breaks=unique(quantile(TARGET_TENENCIA_CUPS, na.rm=T)), include.lowest=T))


compositionPlot <- function(field='CNAE'){
    
    count_field <- usuarios  %>% group_by_("cluster", field) %>% summarize(n_customers=n(), acc_power=sum(POT, na.rm=T))
    dataplot <- count_field  %>% gather_(key_col="var",value_col="valor", gather_cols=c("n_customers", "acc_power")) %>% filter(!is.na(cluster))
    return(
        ggplot(dataplot, aes_string(x=quote(as.factor(cluster)),y="valor", fill=field)) + 
            geom_bar(stat='identity') +
            scale_x_discrete(name="cluster")+
            scale_y_continuous(name="")+
            facet_wrap(~var, nrow=2, scales='free')
    )
}

fieldFrequencies <- function(field='CNAE'){
    #frecuencias totales de clases CNAE
    n_total <- table(usuarios[,field], useNA="ifany")
    freqs_total <- as.data.frame(n_total/margin.table(n_total)) #%>% rename_(field="Var1")
    colnames(freqs_total)[colnames(freqs_total)=='Var1'] <- field
    
    n_total <- as.data.frame(n_total) %>% rename_("n"="Freq")
    colnames(n_total)[colnames(n_total)=='Var1'] <- field
    freqs_total <- left_join(n_total, freqs_total, by=field)
    
    freqs<- usuarios %>% group_by_("cluster", field) %>% summarize(n_cl=n()) %>% mutate(freq_cl=n_cl/sum(n_cl)) %>% 
        left_join(freqs_total, by=field) %>% mutate(freq_dev=freq_cl-Freq)
    return(freqs)
}

clusterFrequencyPlot <- function(field='CNAE'){
    freqs <- fieldFrequencies(field)
    
    freq_dataplot <- freqs %>% select_("cluster", field, "freq_cl") %>% gather_(key_col="var", value_col="valor", "freq_cl") %>% filter(!is.na(cluster))
    
    return(
        ggplot(freq_dataplot , aes_string(x=quote(as.factor(cluster)),y="valor", fill=field)) + 
            geom_bar(stat='identity', position='dodge') +
            scale_x_discrete(name="cluster")+
            scale_y_continuous(name="")+
            ggtitle(sprintf("Within-cluster frequencies for %s", field))
    )       
    
}

frequencyDevPlot <- function(field='CNAE'){
    freqs <- fieldFrequencies(field)
    
    freq_dataplot <- freqs %>% select_("cluster", field, "freq_dev") %>% gather_(key_col="var", value_col="valor", "freq_dev") %>% filter(!is.na(cluster))
    
    return(
        ggplot(freq_dataplot , aes_string(x=quote(as.factor(cluster)),y="valor", fill=field)) + 
            geom_bar(stat='identity', position='dodge') +
            scale_x_discrete(name="cluster")+
            scale_y_continuous(name="")+
            ggtitle(sprintf("Deviation from overall frequencies by cluster for %s", field))
        
    )    
    
}

overallFreqPlot <- function(field='CNAE'){
    freqs <- fieldFrequencies(field)
    
    freq_dataplot <- freqs %>% select_("cluster", field, "Freq") %>% filter(cluster==1) %>% gather_(key_col="var", value_col="valor", "Freq")
    
    return(
        ggplot(freq_dataplot , aes_string(x=quote(as.factor(cluster)), y="valor", fill=field)) + 
            geom_bar(stat='identity', position='dodge', width=0.5) +
            scale_x_discrete(name="", labels="")+
            scale_y_continuous(name="")+
            #facet_wrap(~var, nrow=2, scales='free')
            ggtitle(sprintf("Overall frequencies for %s", field))
        
    )
}

grafico_promedios <- function(dev_data, xval, clusters=NULL){
    
    dev_data <- data.frame(dev_data)
    
    if (!is.null(clusters)){
        cl_plot <- paste0('cl_', clusters)
        dev_data <- dev_data %>% filter(cluster %in% cl_plot)   
    }
    
    minval <- min(dev_data[,xval])
    maxval <- max(dev_data[,xval])
    
    return(
        ggplot(dev_data, aes_string(x=xval, y="value", colour="cluster")) + 
            geom_point() + geom_line()+
            scale_x_continuous(breaks=minval:maxval)+
            scale_y_continuous(name="")
    )
}

dt_series <- data.table(series)[fecha>=fecha_i]

series_wday <- dt_series[, lapply(.SD, mean), by=wday(fecha)]
series_wday_m <- melt(series_wday, id.vars="wday", variable.name = 'cluster')
dias <- as.integer(series_wday[,wday])
mean_dev_wday <- melt(series_wday[, lapply(.SD, function(x) x-mean(x)), .SDcols=-1][,wday:=dias],
                      id.vars='wday', variable.name = 'cluster')

series_h_month <- dt_series[, lapply(.SD, mean), by=.(hour(fecha), month(fecha))]
series_h_month_m <- melt(series_h_month, id.vars=c("hour", "month"), variable.name = 'cluster')
series_month <- dt_series[, lapply(.SD, mean), by=.(month(fecha))]
series_month_m <- melt(series_month, id.vars="month", variable.name = 'cluster')

gr_h_month <- ggplot(series_h_month_m, aes(x=hour, y=value, colour=as.factor(month))) + 
    geom_line() + geom_point(size=1)+
    facet_wrap(~cluster, nrow=5, ncol=4, scales='free') + 
    scale_y_continuous("consumption relative to contracted power")+
    scale_color_brewer(name="month", palette='Spectral')#+
    #ggtitle("Hourly average consumption by month")

meses <- as.integer(series_month[,month])
mean_dev_month <-  melt(series_month[, lapply(.SD, function(x) x-mean(x)), .SDcols=-1][,month:=meses],
                        id.vars='month', variable.name = 'cluster')

