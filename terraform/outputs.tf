################################################################################
# Client Machine (EC2 instance)
################################################################################
output "execute_this_to_access_the_bastion_host" {
  value = "ssh ec2-user@${aws_instance.bastion_host.public_ip} -i cert.pem"
}

output "execute_this_to_access_the_producer_host" {
  value = "ssh ec2-user@${aws_instance.producer.public_ip} -i cert.pem"
}

output "execute_this_to_access_the_consumer_host" {
  value = "ssh ec2-user@${aws_instance.consumer.public_ip} -i cert.pem"
}