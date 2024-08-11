#Examples of Curl Commands Format:
#curl http://localhost:11435/api/generate -d '{"model": "mistral",  "prompt": "Why is the sky blue?", "stream": false}'
#curl http://localhost:11435/api/generate -d '{"model": "llama3",  "prompt": "Suggest me a dream travel destination?", "stream": false}'
#curl http://localhost:11435/api/generate -d '{"model": "moondream",  "prompt": "Tell me a joke", "stream": false}'

import http.server  # to handle HTTP requests and responses
import socketserver  # to create a TCP server
import urllib.request  
import urllib.parse  
import json  

import subprocess
import os
import requests
import time
import threading

# Define global variables
set_timer = False
desire_time = None
Node = None
Jobid = None
SLURM_JOB_PATH = '/home/marjan.moradi/Ollama/slurm-job.sh'

###########################################################################################################

#Get the name and JOBID of running Node. if no node is running the function return None
def get_nodelist():
    # Get the current user
    user = os.environ.get('USER')
    
    # Define the SLURM command with the actual username
    shell_command = ["squeue", "-u", user, "-h", "-O", "NODELIST,State,JOBID"]
    
    # Run the command using subprocess.Popen
    shell_process = subprocess.Popen(shell_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Capture the output and errors
    stdout, stderr = shell_process.communicate()
    
    # Check if the command was successful
    if shell_process.returncode != 0:
        raise Exception(f"Command failed with return code {shell_process.returncode}: {stderr}")
    
    # Split the output into lines
    lines = stdout.splitlines()
    
    # Extract the NODELIST from the output
    nodelist = None
    jobid = None
    for line in lines:
        parts = line.split()
        if len(parts) == 3 and parts[1] == "RUNNING":
            nodelist = parts[0]
            jobid=parts[2]
            break
    return nodelist,jobid

###########################################################################################################

#Check if Ollama is running
def check_connection(node):
    try:
        
        URL = f"http://{node}:11434/"
        response = requests.get(URL)
        if response.status_code == 200:
            #print("connected")
            return True
        else:
            #print(f"Not connected to {URL}, Status code: {response.status_code}")
            return False
    #the server is not reachable
    except requests.ConnectionError:
        #print("Exception")
        #print(f"Connection error to {URL}")
        return False

    except requests.RequestException as e:
        #print(f"Exception for {URL}: {e}")
        return False
        
###########################################################################################################

def run_ollama():

    try:
        # Submit a job 'sbatch-job.sh
        shell_command = ["sbatch",SLURM_JOB_PATH]
        
        # execute a shell command
        shell_process = subprocess.Popen(shell_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
         # Capture the output and errors
        stdout, stderr = shell_process.communicate()
    
         # Check if the command was successful
        if shell_process.returncode != 0:
            raise Exception(f"Command failed with return code {shell_process.returncode}: {stderr}")
        
        # Allow some time for the server to start 
        time.sleep(5)
        
        print("run apptainer")
        
        # Attempt to connect to the server to check if it's running
        Node, Jobid= get_nodelist()

        
        if check_connection(Node):
            print("ollama started successfully.")

        else:
            print("ollama failed to start.")

    except subprocess.CalledProcessError as e:
        print(f"Error running the container: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}") 

###########################################################################################################

def shutdown_ollama(jobid):
    
    shell_command = ["scancel", jobid]
    
    # Run the command using subprocess.Popen
    shell_process = subprocess.Popen(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Capture the output and errors
    stdout, stderr = shell_process.communicate()
    
    # Check if the command was successful
    if shell_process.returncode != 0:
        raise Exception(f"Command failed with return code {shell_process.returncode}: {stderr}")
    
    # Wait for the container to shut down.(it takes 32 seconds)
    print("The container is shutting down.")
    time.sleep(10)
    print("Container shut down successfully.")
###########################################################################################################

def forward_request(request_handler):
    global Node
    # Call the "intercept_request" method on the "request_handler" object.
    request_handler.intercept_request()
    
    # get the size of the request body
    content_length = int(request_handler.headers['Content-Length'])
    
    # Read the request body from the "rfile" attribute of the "request_handler" object.
    # using the "content_length" to determine how many bytes to read.
    post_data = request_handler.rfile.read(content_length)
    
    # Define the new URL to forward the request to(data proxy)
    new_url = f"http://{Node}:11434/api/generate"
    
    # Create a new "Request" object from the "urllib.request" module
    req = urllib.request.Request(new_url, data=post_data, method='POST')

    # Loop through the headers from the original request and add them to the new request.
    # This ensures the new request has the same headers as the original.
    for key, value in request_handler.headers.items():
        req.add_header(key, value)

    # Forward the request and get the response
    try:
        with urllib.request.urlopen(req) as response:
            # Read the response body from the new request and decode it from 'utf-8'
            response_text = response.read().decode('utf-8')
            
            # Parse the JSON response text into a Python dictionary.(Because I want to send only a specific part of the response to the user.)
            data = json.loads(response_text)
            
            # Extract the value associated with the "response" key from the JSON data. 
            actual_response = data.get("response", "")
            
            # Convert the "actual_response" back into a JSON-encoded byte string.
            response_data = json.dumps(actual_response).encode('utf-8')
            
            # Send a HTTP 200 (OK response) to the original request
            request_handler.send_response(200)
            
            # Write the JSON response data to the response output stream.
            request_handler.send_header('Content-Type', 'application/json')
            request_handler.send_header('Content-Length', len(response_data))
            request_handler.end_headers()
            request_handler.wfile.write(response_data)
            
    # Handle the HTTPError that may occur while trying to open the new request URL.
    except urllib.error.HTTPError as e:
        request_handler.send_response(e.code)
        request_handler.end_headers()
        request_handler.wfile.write(e.read())             
             
########################################################################################################### 

# Periodically check if the timer has expired and shut down the container if necessary.
def timer_check():
    
    global set_timer, desire_time, Jobid
    while True:
        current_time = time.time()
        if set_timer and current_time >= desire_time:
            print("Current time:", current_time)
            print("Timer expired, shutting down container.")
            set_timer = False
            desire_time = None
            shutdown_ollama(Jobid)
        # Check every 2 seconds    
        time.sleep(2)   
        
###########################################################################################################  
      
# Function to handle the intercepted requests
def handle_request(request_handler):
    # Declare the global variables
    global set_timer, desire_time, Node, Jobid  
    # Check if the request path is '/api/generate'
    if request_handler.path == "/api/generate":
        Node, Jobid= get_nodelist()
        #Check the connection. If it is connected, generate the response. Otherwise, first establish the connection and then generate the response.
        if not check_connection(Node):
            run_ollama()
            Node, Jobid= get_nodelist()
                
        #Check the connection. If it is connected, generate the response.
        if check_connection(Node):
            forward_request(request_handler)
            #If the timer is not set, set it to 60 seconds.
            if not set_timer:
                set_timer = True
                print("Start time:", time.time())
                desire_time = time.time() + 60
                print("Desired end time:", desire_time)  
            #If the timer is set, reset it to 60 seconds.   
            if set_timer:
                desire_time = time.time() + 60  
                          
    else:
        # For all other paths, respond with 404 Not Found
        request_handler.send_response(404)
        request_handler.end_headers    

###########################################################################################################

# Class to handle HTTP requests as a proxy server
class ProxyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    
    # Method to log intercepted requests
    def intercept_request(self):
        print(f"Intercepted request to: {self.path}")

    # Handle POST requests
    def do_POST(self):
        handle_request(self)

    # Handle GET requests by responding with 404 Not Found
    def do_GET(self):
        self.send_response(404)
        self.end_headers()
        
###########################################################################################################

def main():
    # Define(Changed) the port number for the proxy server
    PORT = 11435 
    
    # Start the timer checking thread (to shut down Ollama as soon as the time is up)
    timer_thread = threading.Thread(target=timer_check, daemon=True)
    timer_thread.start()

    # Create a TCP server that listens on localhost at the specified port(11435)
    with socketserver.TCPServer(("localhost", PORT), ProxyHTTPRequestHandler) as httpd:
        print(f"Serving localhost at port {PORT}")
        # Keep the server running indefinitely
        httpd.serve_forever()

###########################################################################################################
      
if __name__ == '__main__':
    main()
        

