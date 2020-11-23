#!/usr/bin/python3.8

import os
import requests
import argparse
import queue
import threading
import logging



DOWNLOAD_THREADS = 6
IO_THREADS = 2

download_queue = queue.Queue()
write_queue = queue.Queue()

files_lock = threading.Lock()

def download_worker(download_queue, write_queue, url, chunk_size):
    finished = False
    logging.info("Download thread started")
    while not finished:
        chunk_offset = download_queue.get()
        if chunk_offset is not None:
            chunk_end = chunk_offset + chunk_size - 1
            logging.debug(f"GET: {url} - {chunk_offset}-{chunk_end}")
            resume_headers = {}
            if chunk_size != 0:
                resume_headers = {"Range":f"bytes={chunk_offset}-{chunk_end}"}
            r = requests.get(url, stream=True, headers=resume_headers)
            chunk = r.content
            write_queue.put((chunk_offset, chunk_size, chunk))
        else:
            finished = True
        download_queue.task_done()

def io_worker(write_queue, file_size, file_chunk_size, files):
    finished = False
    while not finished:
        task = write_queue.get()
        if task is not None:
            chunk_offset, chunk_size, chunk = task
            file_start = file_chunk_size * (chunk_offset // file_chunk_size)    # Calculate which file the received chunk belongs to
            file_offset = chunk_offset - file_start                 # Calculate where in the file to put the chunk
            
            filename = f"{file_start}.dat"

            logging.debug(f"File: {filename}. {chunk_offset} -> {file_start}+{file_offset}")

            if file_offset + chunk_size > file_chunk_size:                # If not all of the chunk fits in current file, put rest back for later
                
                overflow = file_offset + chunk_size - file_chunk_size
                new_offset = chunk_offset + file_chunk_size - file_offset

                logging.debug(f"File: {filename}. Splitting {overflow} bytes to next file at {new_offset} ")
                write_queue.put((new_offset, overflow, chunk[-overflow:]))
                chunk = chunk[:-overflow]
                chunk_size = file_chunk_size - file_offset
            with files_lock:
                with files[file_start]:
                    if os.path.isfile(filename):
                        with open(filename, "r+b") as f:
                            f.seek(file_offset)
                            f.write(chunk)
                    else:
                        with open(filename, "wb") as f:
                            logging.debug(f"Starting new file {filename}")
                            if file_size < file_start + file_chunk_size:
                                newfile = file_size - file_start
                            else: 
                                newfile = file_chunk_size
                            f.truncate(newfile)
                            f.seek(file_offset)
                            f.write(chunk)
        else:
            finished = True
        write_queue.task_done()



def main(url, http_chunk, file_chunk):
    logging.basicConfig(level=logging.DEBUG, format='%(relativeCreated)6d %(threadName)s %(message)s')

    r = requests.head(url, allow_redirects=True)
    if not r.ok:
        logging.error(f"Server returned {r.status_code}, aborting")
        return
    size = int(r.headers.get("Content-Length", -1))
    ranges = r.headers.get("Accept-Ranges","none")
    if ranges.lower() == "none":
        logging.error("Remote server doesn't support download ranges, aborting")
        return
    
    http_chunks = [i * http_chunk for i in range( 1 + size // http_chunk)]

    file_chunks = {i * file_chunk : threading.Lock() for i in range( 1 + size // file_chunk)}
    
    logging.debug(f"{size}: {http_chunks}")
    for chunk_start in http_chunks:
        download_queue.put(chunk_start)

    download_workers = []
    io_workers = []

    for thread_no in range(DOWNLOAD_THREADS):
        t = threading.Thread(target=download_worker, kwargs={   "download_queue": download_queue,
                                                                "write_queue": write_queue,
                                                                "url": url,
                                                                "chunk_size": http_chunk})
        t.start()
        download_workers.append(t)
        download_queue.put(None)  

    for thread_no in range(IO_THREADS):
        t = threading.Thread(target=io_worker,  kwargs={ "write_queue": write_queue,
                                                        "file_size": size,
                                                        "file_chunk_size": file_chunk, 
                                                        "files": file_chunks},
                                                daemon=True)
        t.start()
        io_workers.append(t)

    download_queue.join()
    write_queue.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL to download")
    parser.add_argument("http_chunk", help="Size (in bytes) of download chunk", type=int)
    parser.add_argument("file_chunk", help="Size (in bytes) of output file chunk", type=int)
    args = parser.parse_args()
    main(args.url, args.http_chunk, args.file_chunk)

