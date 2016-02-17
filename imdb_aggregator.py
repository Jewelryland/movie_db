#!/usr/bin/python2.7
"""Fetch movie information from IMDb.

Author: Rohith Subramanyam <rohithvsm@cs.wisc.edu>

"""

import sys
sys.path.append('../alberanid-imdbpy-398c01b96107/bin')
import imdb
import csv
import traceback
import multiprocessing
import contextlib

in_encoding = sys.stdin.encoding or sys.getdefaultencoding()
out_encoding = sys.stdout.encoding or sys.getdefaultencoding()

def do_work(imdb_obj, done, input_file, output_file, error_file):
    """The task function for the multiple processes which fetches the
    attributes for the movies in the input file and write it as a csv in
    the output file.

    Args:
        imdb_obj   : the imdb object which uses the IMDb HTTP API
        done       : a shared dictionary into which the imdbid of the
                     movie is inserted after fetching its attributes
        input_file : the file from which the list of movie titles is
                     read for querying
        output_file: the csv file into which the movie records are
                     written
        error_file : the file into which the error occuring with each
                     movie is logged

    """

    title_error_count  = 0
    result_error_count = 0
    report_error_count = 0

    with contextlib.nested(open(input_file), open(output_file, 'wb')
                          ,open(error_file, 'wb')) as (in_fo, csvfile, err_fo):
        try:
            out = csv.writer(csvfile)
            header = ['imdbid', 'title','genres','director','writer','cast'
                     ,'runtime', 'country','language','rating','plot']
            out.writerow(header)

            for line in in_fo:
                line_splits = line.rsplit(None, 1)
                title = line_splits[0]
                #year  = line_splits[1]
                in_title = unicode(title, in_encoding, 'replace')

                try:
                    # Do the search, and get the results (a list of Movie objects).
                    results = imdb_obj.search_movie(in_title)

                except imdb.IMDbError:
                    print >> err_fo, in_title
                    traceback.print_exc(file=err_fo)
                    title_error_count += 1
                    continue

                for movie in results:
                    out_title = movie['long imdb title'].encode(out_encoding
                                                               ,'replace')
                    movieID = movie.movieID
                    imdb_id = imdb_obj.get_imdbID(movie)
                    if imdb_id in done: continue

                    try:
                        # Get a Movie object with the data about the movie
                        # identified by the given movieID.
                        movie = imdb_obj.get_movie(movieID)

                    except imdb.IMDbError:
                        print >> err_fo, imdb_id
                        print >> err_fo, out_title
                        traceback.print_exc(file=err_fo)
                        result_error_count += 1
                        continue

                    if not movie:
                        print >> err_fo, 'No movie with movieID: %s' % imdb_id
                        print >> err_fo, out_title
                        result_error_count += 1
                        continue

                    # movie summary report
                    row = [imdb_id]
                    index = 1
                    for line in movie.summary().encode(out_encoding
                                                      ,'replace').split('\n')[2:]:
                        try:
                            if not line: continue
                            line_splits = line.split(':', 1)
                            attr = line_splits[0].lower()
                            if attr != header[index]:
                                while attr != header[index]:
                                    row.append('')
                                    index += 1
                            value = line_splits[1].strip().rstrip('.')
                            row.append(value)
                            index += 1

                        except Exception:
                            print >> err_fo, 'No movie with movieID: %s' % movieID
                            print >> err_fo, imdb_id
                            print >> err_fo, out_title
                            traceback.print_exc(file=err_fo)
                            report_error_count += 1
                            break

                    out.writerow(row)
                    done[imdb_id] = 1

        finally:
            print >> err_fo, 'title_error_count: %d'  % title_error_count
            print >> err_fo, 'result_error_count: %d' % result_error_count
            print >> err_fo, 'report_error_count: %d' % report_error_count

def main():
    """Initialize the IMDb object and spawn the subprocesses."""
    imdb_obj = imdb.IMDb()

    manager = multiprocessing.Manager()
    done = manager.dict()

    # single thread
    #do_work(imdb_obj, done, 'movies.list.segment%d' % num
    #       ,'output%d.csv' % num, 'error%d.log' % num)

    # multi-process
    processes = []
    for num in range(1, 9):
        processes.append(multiprocessing.Process(target=do_work, args=(imdb_obj
                ,done, 'movies.list.segment%d' % num,'output%d.csv' % num
                ,'error%d.log' % num)))

    for process in processes:
        process.start()

    for process in processes:
        process.join()

if __name__ == '__main__':
    main()
