data "aws_ami" "amazon" {
  most_recent = true

  filter {
    name   = "image-id"
    values = ["ami-0a252cea0ff9a3e6c"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  efs_mount_point = "/mnt/solr"
}

resource "aws_instance" "efs_interface" {
  ami                    = data.aws_ami.amazon.id
  instance_type          = "t2.nano"
  vpc_security_group_ids = [aws_security_group.solr.id]
  key_name               = "${var.stack_name}-pep-${var.env}"
  subnet_id              = data.aws_subnets.private.ids[0]

  user_data = <<-EOF
                #cloud-config
                package_update: true
                package_upgrade: true
                runcmd:
                - yum install -y amazon-efs-utils
                - apt-get -y install amazon-efs-utils
                - yum install -y nfs-utils
                - apt-get -y install nfs-common
                - file_system_id_1=${local.efs_mount_point}
                - efs_mount_point_1=${module.efs.id}
                - mkdir -p "${local.efs_mount_point}"
                - test -f "/sbin/mount.efs" && printf "\n${module.efs.id}:/ ${local.efs_mount_point} efs tls,_netdev\n" >> /etc/fstab || printf "\n${module.efs.id}.efs.${var.aws_region}.amazonaws.com:/ ${local.efs_mount_point} nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport,_netdev 0 0\n" >> /etc/fstab
                - test -f "/sbin/mount.efs" && grep -ozP 'client-info]\nsource' '/etc/amazon/efs/efs-utils.conf'; if [[ $? == 1 ]]; then printf "\n[client-info]\nsource=liw\n" >> /etc/amazon/efs/efs-utils.conf; fi;
                - retryCnt=15; waitTime=30; while true; do mount -a -t efs,nfs4 defaults; if [ $? = 0 ] || [ $retryCnt -lt 1 ]; then echo File system mounted successfully; break; fi; echo File system not available, retrying to mount.; ((retryCnt--)); sleep $waitTime; done;
                EOF

  tags = {
    Name  = "${var.stack_name}-solr-efs-interface-${var.env}"
    stack = var.stack_name
    env   = var.env
  }
}

resource "aws_ec2_instance_state" "stop" {
  instance_id = aws_instance.efs_interface.id
  state       = "stopped"
}
