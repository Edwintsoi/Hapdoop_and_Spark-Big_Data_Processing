library(dplyr)
library(ggplot2)
library(lubridate)

ransomeware <- read.csv("~/Downloads/result", header=FALSE)
colnames(ransomeware) = c("tx_id","time","pubkey","value")

ransomeware$tx_id = gsub("\\[|\\]", "", ransomeware$tx_id)
ransomeware$value = gsub(']', '', ransomeware$value)
ransomeware$time = as.Date(as.POSIXct(ransomeware$time, origin="1970-01-01"))
ransomeware$date =round_date(ransomeware$time, "week")
ransomeware$value = as.double(ransomeware$value)

ransomeware = filter(ransomeware, date > ymd("2013-09-01"), date < ymd("2014-06-01") )
ransomeware = filter(ransomeware, value >= 0.3, value <= 2 )
str(ransomeware)

count(ransomeware)
sum(ransomeware$value)

graph1 = ransomeware %>% 
          group_by(date) %>% 
            summarise(total_sum = sum(value), count = n())

payment_per_address = ransomeware %>% 
                        group_by(pubkey) %>% 
                          summarise(payment_count = n(), ransom_sum = sum(value))

head(arrange(payment_per_address, desc(payment_count)),20)

graph1$cumsum = cumsum(graph1$total_sum)

ggplot() + geom_line(data = graph1, aes(y = total_sum, x = date))



