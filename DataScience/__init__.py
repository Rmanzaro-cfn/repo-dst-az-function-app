import logging
import os
import numpy as np 
import re 
from textblob import TextBlob
import snowflake
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import azure.functions as func

user_name = os.getenv('PYTHON-TO-SNOWFLAKE-USERNAME')
password = os.getenv('PYTHON-TO-SNOWFLAKE-PASSWORD')



def classify(polscore):
    if polscore >= 0.4:
        return "Positive"
    elif polscore >= .12:
        return "Neutral"
    else:
        return "Negative"

def sentiment_main(user, pw):


    conn = snowflake.connector.connect(
    authenticator='snowflake',
    user=user,
    account='qi10407.east-us-2.azure',
    password= pw,
    warehouse='PROD_ANALYTICS_WH',
    database='PROD_ANALYTICS',
    schema='DATA_SCIENCE'
    )

    cur= conn.cursor()

    # Execute a statement that will generate a result set.

    sql = """SELECT 

        f.feedbackid as "FeedbackId"
        ,f.auditid as "AuditId"
        ,f.advisorid as "BoatsID"
        ,concat(f.auditid, 'B', f.advisorid) as "MergeID"
        ,f.feedbacktext as "FeedbackText"
        ,f.categoryid as "CategoryId"
        ,a.CompletedDate as "CompletedDate"
        ,a.ScheduledDate as "ScheduledDate"
        ,a.AuditLetterSentDate as "AuditLetterSentDate"
    
    
    FROM PROD_ANALYTICS.DATA_SCIENCE.FEEDBACK f
    
    left join PROD_ANALYTICS.DATA_SCIENCE.AUDIT a on f.AuditId = a.ID

    where f.feedbacktext not like 'test%' and f.feedbacktext is not NULL and f.feedbacktext not like 'n/a%'
    and a.completeddate is not null
    order by a.auditlettersentdate"""

    cur.execute(sql)

    # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.

    dfin = cur.fetch_pandas_all()

    try:
        cur.close()

    except:
        pass


    dfin = dfin[dfin['FeedbackText'].map(len) > 4]


    polarity = lambda x: TextBlob(x).sentiment.polarity
    dfin["Polarity"] = dfin["FeedbackText"].apply(polarity)




    sent_class = lambda x: classify(x)
    dfin["Sentiment"] = dfin["Polarity"].apply(sent_class)


    dfin = dfin.round({'Polarity':5})

    dfins = dfin[["FeedbackId", "Polarity", "Sentiment"]]

    table_sql="""
        CREATE OR REPLACE TABLE "BFEEDBACK" 
        ("FeedbackId" INT, "AuditId" INT, "BoatsID" INT, "MergeID" VARCHAR,
        "FeedbackText" VARCHAR, "CategoryId" FLOAT,
        "CompletedDate" DATETIME, "ScheduledDate" DATETIME, "AuditLetterSentDate" DATETIME,
        "Polarity" FLOAT, "Sentiment" VARCHAR)

    """
    cur = conn.cursor()
    cur.execute(table_sql)

    try:
        cur.close()

    except:
        pass

    bsent_sql = 'CREATE OR REPLACE TABLE bFBSent ("FeedbackId" INT,"Polarity" FLOAT, "Sentiment" VARCHAR)'

    cur = conn.cursor()
    cur.execute(bsent_sql)

    try:
        cur.close()

    except:
        pass


    success, nchunks, nrows, _ = write_pandas(conn, dfin, 'BFEEDBACK')
    success, nchunks, nrows, _ = write_pandas(conn, dfins, 'BFBSENT')


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    sentiment_main(user_name, password)
    


    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )
