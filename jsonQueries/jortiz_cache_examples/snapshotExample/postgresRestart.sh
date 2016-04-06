host="ec2-50-19-17-5 ec2-54-226-66-48 ec2-54-90-109-95 ec2-54-91-28-12 ec2-54-160-147-159"

CMD="sudo service postgresql restart"
parallel -k --jobs +28 "/bin/echo -n '{} -- ' && ssh -i ~/.ssh/jortiz16Key.rsa ubuntu@{}.compute-1.amazonaws.com -o ConnectTimeout=6 '$CMD' 2>&1" ::: $host
