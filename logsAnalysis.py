#!/usr/bin/env python3

__version__ = '1.0'
__author__ = "Mohamed Fawzy"

import psycopg2

# Database name
dbname = "news"


def main():
    """This is the main function which calls other functions
     to display the output """
    #   Database Connection
    db = psycopg2.connect(database=dbname)
    # Database Cursor
    cursor = db.cursor()
    # Question - 1
    popular_articles_query = """
        select articles.title, count(*) as view
        from articles, log
        where log.path like concat('/article/%', articles.slug)
        group by articles.title
        order by view desc
        limit 3;
    """
    cursor.execute(popular_articles_query)
    print("1- Most popular articles: ")
    print(' ')
    for (title, view) in cursor.fetchall():
        print("    {} -- {} views".format(title, view))
    print(' ')
    print(' ')

    # Question - 2
    popular_articles_authors_query = """
        select authors.name, count(*) as num
        from authors, articles, log
        where authors.id = articles.author
        and log.path like concat('/article/%', articles.slug)
        group by authors.name
        order by num desc;
    """
    cursor.execute(popular_articles_authors_query)
    print("2- Most popular authors: ")
    print(' ')
    for (name, num) in cursor.fetchall():
        print("    {} -- {} views".format(name, num))

    print(' ')

    # Question - 3
    days_with_errors_query = """
        select total_days.day,
         round(((errors.error_requests*1.0) / total_days.requests) * 100, 2)
          as percentage
        from (
          select cast(time as date) as "day", count(*) as error_requests
          from log
          where status like '404%'
          group by day ) as errors
        join (
          select cast(time as date) as "day", count(*) as requests
          from log
          group by day ) as total_days
        on total_days.day = errors.day
        where (
          round(((errors.error_requests*1.0) / total_days.requests), 2)> 0.01)
        order by percentage desc;
    """

    print(' ')
    print(' ')

    cursor.execute(days_with_errors_query)
    print("3- Days with more than 1% errors: ")
    print(' ')
    for (day, percentage) in cursor.fetchall():
        print("    {} -- {}% errors".format(day, percentage))

    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
