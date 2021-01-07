so we have to load the s3 data into staging first
i don't understand what that practically means yet though
then from staging we insert that data into sql tables on redshift

Staging table is a temporary table that is used to stage the data for temporary purpose just before loading it to the Target table from the Source Table.




okay so for staging ... get it from raw .json to the redshift staging table
exactly as it currently is. all the fields and junk

then from that staging table we'll shape it into the final star schema




okay so now we have all data in two tables
i suppose we could do a select * on those tables
but those selects might be very process heavy 
how else could we approach this?
is there any way to break this apart?

well hmmmm


# make sure to add how many records i'm working with to show off big data skills