require 'bundler/setup'
require 'sponge_discovery'

aws_region = ENV['AWS_REGION']
vpc_name = ENV['SPONGE_VPC_NAME']
deploy_environment = ENV['DEPLOYMENT']
role = ENV['SPONGE_ROLE']
node_name = ENV['SPONGE_NODE_NAME']

sponge_vpc = SpongeDiscovery::SpongeVPC.new(aws_region, vpc_name, deploy_environment)

cloudformation_parameters = {
  "AvailabilityZone" => sponge_vpc.first_external_subnet.availability_zone_name,
  "ClusterSize" => (ENV['CLUSTER_SIZE'] or "1"),
  "DeployEnvironment" => deploy_environment,
  "DnsZone" => sponge_vpc.route53_zone + ".",
  "ImageId" => sponge_vpc.find_ami_by_name("sponge_ubuntu_1404_base_hvm-1.6.1"),
  "InstanceType" => (ENV['INSTANCE_TYPE'] or "t2.medium"),
  "KeyName" => sponge_vpc.default_keypair,
  "SecurityGroupId" => sponge_vpc.default_external_security_group.id,
  "Subnet" => sponge_vpc.first_external_subnet.subnet_id,
  "VpcId" => sponge_vpc.vpc.id,
  "VpcName" => sponge_vpc.name,
  "NodeName" => node_name,
  "SNSAlarm" => sponge_vpc.snsalarm
}

aws_params = []
cloudformation_parameters.each do |k,v|
  aws_params << {"ParameterKey" => k,"ParameterValue"=> v}
end

puts JSON.pretty_generate(aws_params)
