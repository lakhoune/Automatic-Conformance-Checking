
import numpy as np
import pandas as pd
from pycelonis import get_celonis
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter

celonis_url = "academic-m-s-qafari-pads-rwth-aachen-de.eu-2.celonis.cloud"
api_token = "TUxZGNjNmItYzEwNy00MTI4LWJjZDctZmU1Zjg0Y2ZiYmQ0OkxRaHNRaEd6eHFMYXBwSlhyUkg1Z0NlUjBOUDlMbzdpcFZNNGx4cGdhdlJx"

try:
    celonis = get_celonis(celonis_url=celonis_url, api_token=api_token)
except:
    print("error") 
