# LAB 2 PR: Concurrent HTTP server
During this laboratory work I modified the server from the first lab and it became multithreaded, so it handles multiple connections concurrently. It also has a counter and a rate limit of 5 req/s.

## Contents of Directory
* I have multiple directories in the root. The *Public* and *Downloads* directories and their subdirectories are used for testing the server and client functionality. *Public* contains various nested directories, as well as some html, png and pdf files for testing. The other nested directories(*books, docs*) also feature png, html and pdf files for testing. *Report_pics* directory contains images used in this report.
* In this project, the *Dockerfile* defines how the server and request script environments are built, including the necessary dependencies and instructions to run the Python applications inside containers. The *docker-compose.yml* file is used to coordinate these containers, starting both the server and requests script, linking them together, mapping the appropriate ports, and managing shared volumes.
* The *server_mt.py* file handles incoming HTTP requests, retrieves the requested files from the specified directory, and sends them back to the client. It also handles the threads, counter for requests and rate limiting.
* The *request_test.py* connects to the server, sends file requests, and displays the statistics regarding the requests.

    ![img_8.png](public%2Freport_pics%2Fimg_8.png)

## Dockerfile
The `Dockerfile` sets up a lightweight Python 3.12-slim environment, creates the `/app` working directory, copies in `server_mt.py` and `request_test.py`, defines port `8001` as an environment variable and exposes it, then runs `server_mt.py` with `/serve` as the directory to serve files from when the container starts.
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY server_mt.py request_test.py ./
ENV PORT=8001
EXPOSE 8001
CMD ["python", "server_mt.py", "/serve"]
```

## Docker compose
This `docker-compose.yml` file defines two services: a multithreaded server that runs `server_mt.py` on port `8001` to serve files from the mounted directory, and a `requesttest` client that executes `request_test.py` to send requests to the server, both sharing the same build context and Dockerfile.
```dockerfile
services:
  server2:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pr-web-server-lab2
    environment:
      PORT: 8001
    ports:
      - "8001:8001"
    volumes:
      - ./:/serve:ro
    command: ["python", "server_mt.py", "/serve"]

  requesttest:
    build:
      context: .
      dockerfile: Dockerfile
    entrypoint: [ "python", "request_test.py" ]

```

## Running the project
We can run the project locally using the command:
```python
python3 server_mt.py <path_to_directory>
```
For example to run in the root director y(from lab2 directory) of the project we can use:
```python
 python3 server_mt.py ./
```

We can also run the project using docker with the following command:
```
docker compose up server2
```
At the moment it runs in root, but we can change that in the docker-compose.yml file.

When we run the server we get in the browser:
![img_9.png](public%2Freport_pics%2Fimg_9.png)

## Compare single threaded and multi threaded server
First I ran both servers, then I ran the following command to test both server by sending 10 concurrent requests using the `request_test.py` script. Both servers have a 0.5s delay when processing requests to better see the difference.
```
docker compose run --rm requesttest host.docker.internal 8000 public/flower.png 10 
```
For the single threaded server the output is:
![img_4.png](public%2Freport_pics%2Fimg_4.png)
For the multi threaded server the output is:
![img_5.png](public%2Freport_pics%2Fimg_5.png)
We can see that the multi threaded server handled the requests much faster, since there is a new request for each request, while in the single threaded server the requests had to wait for the previous one to finish and there is just one thread.

## Counter
I also implemented a counter, first in a naive way, with delays so the race condition would be noticable. I ran this command:
![img_2.png](public%2Freport_pics%2Fimg_2.png)
And in the browser I didn't get the number of requests I actually sent (200) in the Hits column, but a much lower number-14. This happened because of the race condition, where multiple threads accessed and modified the counter variable simultaneously, leading to lost updates and an inaccurate count of total requests handled by the server.
![img.png](public%2Freport_pics%2Fimg.png)
I then updated the code to use a lock, and ran the same command again and in the next screenshot you can see that I actually got 200 requests this time.
![img_3.png](public%2Freport_pics%2Fimg_3.png)

## Rate limiting
I also implemented rate limiting, so that the server can handle only 5 requests per second. If more requests are sent in that time frame, they will get a 429 error. I ran this command:
![img_7.png](public%2Freport_pics%2Fimg_7.png)
We can see that when I sent 200, I got 5 successful ones and the other 195 were rate limited with error 429.
When sending the requests using my ```request_test.py``` script we can also add a delay for the requests to be sent, so they can be spaced out and not all sent at once. I ran this command:
![img_6.png](public%2Freport_pics%2Fimg_6.png)
By sending a delay of 0.25 I made sure that only 4 requests are sent per second, so all 200 requests were successful since they did not pass the 5 requests limit.

Also my colleague Daniel Cojocaru tried to send more than 5 req to my server in a second and he got a 429 error as well in browser:
![img_10.png](public%2Freport_pics%2Fimg_10.png)
When he sent less than 5 req/s he got a 200 OK response:
![img_11.png](public%2Freport_pics%2Fimg_11.png)