import os, sys, socket, mimetypes
from urllib.parse import unquote, quote
import threading
import time
from typing import Dict, List

# config
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8001"))
ALLOWED_EXTENSIONS = {".html", ".png", ".jpg", ".pdf"}

# Files to show in directory listing
VISIBLE_FILES = {"index.html", "Syllabus PR FAF-23x -2.pdf"}
VISIBLE_DIRS = {"books", "docs", "mercedes", "report_pics"}
INCLUDED_FILES = {"index.html", "Syllabus PR FAF-23x -2.pdf"}
INCLUDED_DIRS = {"books", "docs", "report_pics"}
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "16"))
COUNTS: Dict[str, int] = {}
COUNTS_LOCK = threading.Lock()
REQUESTS_PER_SECOND = 5
TIME_WINDOW = 1.0


client_requests: Dict[str, List[float]] = {}
requests_lock = threading.Lock()

# ensure common types exist
mimetypes.init()
mimetypes.add_type("application/pdf", ".pdf")
mimetypes.add_type("image/png", ".png")
mimetypes.add_type("image/jpeg", ".jpg")
mimetypes.add_type("text/html; charset=utf-8", ".html")


def file_size(num_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} TB"


def _bump_count(path_key: str):
    with COUNTS_LOCK:
        current = COUNTS.get(path_key, 0)
        time.sleep(100 / 1000.0)
        COUNTS[path_key] = current + 1


def respond(conn, status, headers, body):
    head = [f"HTTP/1.1 {status}".encode()]
    for k, v in headers.items():
        head.append(f"{k}: {v}".encode())
    head.append(b"")
    head.append(b"")
    conn.sendall(b"\r\n".join(head) + body)


def _is_subpath(child: str, parent: str) -> bool:
    child_real = os.path.realpath(child)
    parent_real = os.path.realpath(parent)
    try:
        return os.path.commonpath([child_real, parent_real]) == parent_real
    except ValueError:
        return False


def allow_request(ip: str) -> bool:
    #  Check if request from IP should be allowed based on rate limit
    now = time.time()

    with requests_lock:
        if ip not in client_requests:
            client_requests[ip] = []

        timestamps = client_requests[ip]

        # Clean old timestamps beyond window
        client_requests[ip] = [t for t in timestamps if now - t < TIME_WINDOW]

        # Check limit
        if len(client_requests[ip]) < REQUESTS_PER_SECOND:
            client_requests[ip].append(now)
            return True
        return False


def _respond_429(conn):
    body = b"""<!DOCTYPE html>
    <html>
    <head>
        <title>429 Too Many Requests</title>
    </head>
    <body>
        <h1>429 Too Many Requests</h1>
        <p>Please slow down and try again later.</p>
    </body>
    </html>"""
    respond(conn, "429 Too Many Requests",
            {"Content-Type": "text/html; charset=utf-8",
             "Retry-After": "1",
             "Content-Length": str(len(body)), "Connection": "close"}, body)


def _minimal_listing_html(req_path: str, abs_dir: str) -> bytes:
    try:
        entries = sorted(os.listdir(abs_dir))
    except OSError:
        return b"<html><body><h1>Forbidden</h1></body></html>"

    lines = [
        "<!DOCTYPE html>", "<html lang='en'>", "<head>",
        "<meta charset='utf-8'>", "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"<title>Content of {req_path}</title>",
        "</head>", "<body>",
        "<h1>Filip</h1>",
        f"<h2>Content of {req_path}</h2>",
        "<main>",
    ]
    if req_path != "/":
        parent = req_path.rstrip("/").rsplit("/", 1)[0]
        parent = "/" if not parent else parent + "/"
        lines.append(f'<a href="{quote(parent)}">⬆ Parent directory</a>')
    lines.extend(["<table border='1'>", "<tr><th>Name</th><th>Size</th><th>Hits</th></tr>"])
    for name in entries:
        full = os.path.join(abs_dir, name)
        is_directory = os.path.isdir(full)
        # Only filter on main /public/ page
        if req_path == "/" or req_path == "/public/":
            if is_directory:
                if name not in VISIBLE_DIRS:
                    continue
            else:
                if name not in VISIBLE_FILES:
                    continue
        if is_directory:
            href = quote(name) + "/"
            size = "—"
        else:
            href = quote(name)
            size = file_size(os.path.getsize(full))
        child_req_path = req_path + (name + "/" if is_directory else name)
        hits = COUNTS.get(child_req_path, 0)
        lines.append(
            f'<tr><td><a href="{href}">{name if not os.path.isdir(full) else name + "/"}</a></td>'
            f"<td>{size}</td><td>{hits}</td></tr>"
        )
    lines.append("</table></main></body></html>")
    return "\n".join(lines).encode("utf-8")


def _respond_301(conn, location: str):
    body = (f'<html><body>Moved: <a href="{location}">{location}</a></body></html>').encode("utf-8")
    respond(conn, "301 Moved Permanently",
            {"Location": location, "Content-Type": "text/html; charset=utf-8",

             "Content-Length": str(len(body)), "Connection": "close"}, body)


def _respond_404(conn):
    body = b"""<!DOCTYPE html>
    <html>
    <head>
        <title>404 Not Found</title>
    </head>
    <body>
        <h1>404 Not Found</h1>
        <p>The requested page does not exist.</p>
        <a href="/">Return to homepage</a>
    </body>
    </html>"""
    respond(conn, "404 Not Found",
            {"Content-Type": "text/html; charset=utf-8",
             "Content-Length": str(len(body)), "Connection": "close"}, body)


# multithreaded handler
def _serve_connection(conn: socket.socket, addr, content_dir: str):
    # Multithreaded handler with rate limiting
    try:
        client_ip = addr[0]

        # Check rate limit
        if not allow_request(client_ip):
            _respond_429(conn)
            return

        time.sleep(0.5)  # simulate work
        data = conn.recv(4096)
        if not data:
            return

        line = data.split(b"\r\n", 1)[0].decode(errors="replace")
        parts = line.split()
        if len(parts) != 3:
            respond(conn, "400 Bad Request",
                    {"Content-Type": "text/plain", "Connection": "close"},
                    b"Bad Request")
            return

        method, target, version = parts
        if method != "GET":
            respond(conn, "405 Method Not Allowed",
                    {"Allow": "GET", "Content-Type": "text/plain", "Connection": "close"},
                    b"Only GET is allowed")
            return

        if not target.startswith("/"):
            target = "/"
        target = unquote(target)
        _bump_count(target)

        # map to filesystem under content_dir
        # Always serve from /public/ as root
        public_root = os.path.join(content_dir, "public")
        requested_rel = target.lstrip("/")
        requested_abs = os.path.realpath(os.path.join(public_root, requested_rel))

        # 1) traversal guard
        if not _is_subpath(requested_abs, content_dir):
            _respond_404(conn)
            return

        # 2) directory
        if os.path.isdir(requested_abs):
            if not target.endswith("/"):
                _respond_301(conn, target + "/")
                return
            body = _minimal_listing_html(target, requested_abs)
            respond(conn, "200 OK",
                    {"Content-Type": "text/html; charset=utf-8",
                    "Content-Length": str(len(body)), "Connection": "close"},
                    body)
            return

        # 3) file
        if not os.path.isfile(requested_abs):
            _respond_404(conn)
            return

        ext = os.path.splitext(requested_abs)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            _respond_404(conn)
            return

        mime_type, _ = mimetypes.guess_type(requested_abs)
        if mime_type is None:
            _respond_404(conn)
            return

        try:
            with open(requested_abs, "rb") as f:
                body = f.read()
            respond(conn, "200 OK",
                    {"Content-Type": mime_type,
                    "Content-Length": str(len(body)), "Connection": "close"},
                    body)
        except OSError:
            respond(conn, "500 Internal Server Error",
                    {"Content-Type": "text/plain", "Connection": "close"},
                    b"Internal Server Error")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def main():
    if len(sys.argv) != 2:
        print("Usage: python server_mt.py <directory>")
        sys.exit(1)
    content_dir = os.path.abspath(sys.argv[1])
    if not os.path.isdir(content_dir):
        print(f"Error: Directory '{content_dir}' does not exist.")
        sys.exit(1)
    
    # Always serve from the public directory under the content directory
    public_dir = os.path.join(content_dir, "public")
    if not os.path.isdir(public_dir):
        if not os.path.isdir(content_dir):
            print(f"Error: Directory '{content_dir}' does not exist.")
            sys.exit(1)

    print(f"Serving directory (MT - Thread per request): {content_dir}")
    print(f"Server running on: http://0.0.0.0:{PORT}")
    print("Press Ctrl+C to stop")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        
        try:
            while True:
                conn, addr = s.accept()
                # Create a new thread for each request
                thread = threading.Thread(
                    target=_serve_connection, 
                    args=(conn, addr, content_dir),
                    daemon=True
                )
                thread.start()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            sys.exit(0)


if __name__ == "__main__":
    main()
