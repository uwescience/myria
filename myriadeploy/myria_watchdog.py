#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import cgi
import sys
import pickle
import subprocess
import time
import socket
import os.path
import threading

known_deployments = {}

def check_secret_code(form, deployment, self):
    if "secret_code" in form.keys():
        proposed_secret_code = form['secret_code'].value
    else:
        proposed_secret_code = None
    if "secret_code" in deployment.keys():
        correct_secret_code = deployment["secret_code"]
    else:
        correct_secret_code = None
    if not correct_secret_code == proposed_secret_code:
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write("Wrong serect code!")
        return False
    return True

def check_argument(form, key, self):
    if not key in form.keys():
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write("Missing argument: " + key)
        return False
    return True

class myHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/deployments":
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(str(known_deployments.keys()))
            return

    def do_POST(self):
        form = cgi.FieldStorage(
            fp = self.rfile,
            headers = self.headers,
            environ = {'REQUEST_METHOD':'POST',
                       'CONTENT_TYPE':self.headers['Content-Type']
                      })
        if not check_argument(form, 'master', self):
            return
        master = form['master'].value

        if self.path == "/restart":
            # use IP address as the key since a machine can have multiple machine names
            host = socket.gethostbyname(master.split(":")[0])
            port = master.split(":")[1]
            master = host + ":" + port

            if not master in known_deployments.keys():
                self.send_response(200)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write("Unknown deployment!")
                return

            working_dir = known_deployments[master]["working_dir"]
            deployment_file = known_deployments[master]["deployment_file"]
            if not check_secret_code(form, known_deployments[master], self):
                return

            args = ["ssh", host, "cd", working_dir, "&& cd .. && ./stop_all_by_force.py", deployment_file]
            subprocess.call(args)
            args = ["ssh", host, "cd", working_dir, "&& cd .. && ./launch_cluster.sh", deployment_file]
            subprocess.call(args)

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write("Restarted successfully.");
            return

        elif self.path == "/register":
            if not check_argument(form, 'working_dir', self):
                return
            if not check_argument(form, 'deployment_file', self):
                return

            if master in known_deployments.keys():
                # existed deployment, check if the code matches
                if not check_secret_code(form, known_deployments[master], self):
                    return
            known_deployments[master] = {}
            known_deployments[form['master'].value]["working_dir"] = form['working_dir'].value
            known_deployments[form['master'].value]["deployment_file"] = form['deployment_file'].value
            if "secret_code" in form.keys():
                known_deployments[form['master'].value]["secret_code"] = form['secret_code'].value
            self.send_response(200)
            return

        elif self.path == "/unregister":
            # if you need to change your secret code, unregister first
            if master not in known_deployments.keys():
                self.send_response(200)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write("Unknown deployment!")
                return
            if not check_secret_code(form, known_deployments[master], self):
                return
            del known_deployments[master]
            self.send_response(200)
            return

def dumpDeployment():
    while True:
        pickle.dump(known_deployments, open("myria_watchdog_dump", "wb"))
        time.sleep(10)

def loadDeployment():
    if os.path.exists("myria_watchdog_dump"):
        global known_deployments
        known_deployments = pickle.load(open("myria_watchdog_dump", "rb"))

def main(argv):
    # Usage
    if len(argv) > 2:
        print >> sys.stderr, "Usage: %s <port_number>" % (argv[0])
        print >> sys.stderr, "\tport_number: optional, using 8385 if not specified"
        sys.exit(1)

    if len(argv) == 2:
        port_number = int(argv[1])
    else:
        port_number = 8385

    loadDeployment()

    t = threading.Thread(target=dumpDeployment)
    t.daemon = True
    t.start() 

    server = HTTPServer(('', port_number), myHandler)
    print 'Started watchdog on port ' , port_number
    server.serve_forever()

if __name__ == "__main__":
    main(sys.argv)
