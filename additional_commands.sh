docker exec -it broker kafka-topics --bootstrap-server broker:9092 --create --topic esewa_dev.crs_config.cfg_clients --partitions 3 --replication-factor 1

#check if data are streamed to kafka topic or not
#docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 --topic esewa_dev.public.cfg_clients --from-beginning

#check if changes are applied to source db
#docker exec -it postgres psql -U admin -d test_db -c "SHOW wal_level;"
#docker exec -it postgres psql -U admin -d test_db -c "SELECT * FROM pg_publication;"

#change ownership for folder for data in spark.
sudo chown -R 1000:100 ./notebooks ./other_data 

# if we deleted the topic, and the topic recreate but different compact policy, we need to change it.
docker exec -it broker \
  kafka-configs --bootstrap-server broker:9092 \
    --entity-type topics \
    --entity-name docker-connect-offsets \
    --alter \
    --add-config cleanup.policy=compact

docker exec -it broker \
  kafka-configs --bootstrap-server broker:9092 \
    --entity-type topics \
    --entity-name docker-connect-status \
    --alter \
    --add-config cleanup.policy=compact

docker exec -it broker \
  kafka-configs --bootstrap-server broker:9092 \
    --entity-type topics \
    --entity-name docker-connect-configs \
    --alter \
    --add-config cleanup.policy=compact