
# Lectura de los ficheros de perfiles finales de tarificación descargados de la web de REE
rm(list=ls());gc()
#Ejemplo Enero 2015
library(dplyr)
library(tidyr)
library(lubridate)
source('../funciones_tratamiento_datos.R')

dat=read.csv('../datos/PERFF_201501.txt',header=T, sep=";")
dat <- dat %>% mutate(date=as.POSIXct(paste(ANHO, MES, DIA, HORA), format='%Y %m %d %H')) %>% select(date, COEF..PERFIL.A) %>% mutate(date=date-hours(1))

series <- LeeCentroidesClusters('../../spark/output/centroides_kmeans.csv') %>% select(date=fecha, cl_17)

price <- read.csv('../datos/TerminoDeFacturacionDeEnergíaActivaDelPVPCPeajePorDefecto.csv', header=T, sep=';') %>% 
    select(date=datetime, price=value) %>% mutate(date=as.POSIXct(date, format='%Y-%m-%dT%H:00:00+01:00'))


dat <- left_join(dat, series, by='date')
dat <- left_join(dat, price, by='date')

transf <- dat %>% summarize(k=mean(cl_17, na.rm=T)/mean(COEF..PERFIL.A, na.rm=T)) %>% as.numeric

#dygraph_df(dat) %>% dySeries("cl_17", axis='y2')

library(ggplot2)

dat_m <- dat %>% mutate(REE= COEF..PERFIL.A * transf, price=price/1000) %>% select(date, REE, cl_17, price) %>% gather(profile, valor, -date)
gr <- ggplot(dat_m, aes(x=date, y=valor, colour=profile)) + geom_line()


gr <- gr + theme(plot.title=element_text(face="bold"))
gr <- gr + theme(plot.subtitle=element_text(margin=margin(b=15), size=8))

tt <- "Consumption Profiles for January 2015"
st <- "REE: official final profile for non-hourly discrimination used to apply market prices to customers without smart metering device. Cl_17: profile of the cluster grouping the highest number of customers. REE series has been scaled to visually compare both profiles. Price: small consumer market price (eur/kWh)"
st <- paste0(strwrap(st, 110), sep="", collapse="\n")

grafico <- gr + 
    scale_y_continuous(name="")+
    ggtitle(tt, subtitle=st)