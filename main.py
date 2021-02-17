import json
import datetime
import os
from urllib.parse import urlparse
import requests
import looker_sdk
from looker_sdk import models

sdk = looker_sdk.init31()

def r_handle(s, b):
    return json.dumps({
        "statusCode": s,
        "body": {
        "message": b
        }
    })

def action_list(request):
	r = request.get_json()
	print(r) # so it shows up in logs
    
	response = """
        {
       	"integrations": [{
       		"name": "submit_forecast",
          "label": "Submit Forecast",
       		"description": "Submit an approved forecast.",
       		"supported_action_types": ["query"],
          "supported_formats": ["csv"],
       		"url": "https://us-central1-bryce-personal-250216.cloudfunctions.net/take-two/execute",
       		"form_url": "https://us-central1-bryce-personal-250216.cloudfunctions.net/take-two/action_form",
          "icon_data_uri": "data:image/png;base64,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"
       	}]
       }
	"""
	return response

def action_form(request):
    print('loading action form')
    form = [{
      "name": "title",
      "label": "Title",
      "description": "Game Title",
      "type": "text",
      "default": "",
      "required": True
    }, {
      "name": "platform",
      "label": "Platform",
      "description": "Additional context for the forecast",
      "type": "select",
      "options": [
        { "name": "XB1" ,"label": "XB1" },
        { "name": "PS4" ,"label": "PS4" }
      ],
      "default": "XB1",
      "required": True
    }]
    return json.dumps(form)


def put_object_s3(bucket_name, data):
    import boto3
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, 'filename.json').put(Body=bytes(json.dumps(data).encode('UTF-8')))
  

def get_look_query(id: int) -> models.Query:
    """Returns the query associated with a given look id."""
    look = sdk.look(id)
    query = look.query
    assert isinstance(query, models.Query)
    return query


def run_query_with_filter(query: models.Query, filters):
    """Runs the specified query with the specified filters."""
    request = create_query_request(query, filters)
    json_ = sdk.run_inline_query("json", request, cache=False)
    json_resp = json.loads(json_)
    return json_resp


def create_query_request(q: models.Query, filters) -> models.WriteQuery:
    return models.WriteQuery(
        model=q.model,
        view=q.view,
        fields=q.fields,
        pivots=q.pivots,
        fill_fields=q.fill_fields,
        filters=filters,
        sorts=q.sorts,
        limit=q.limit,
        column_limit=q.column_limit,
        total=q.total,
        row_total=q.row_total,
        subtotals=q.subtotals,
        dynamic_fields=q.dynamic_fields,
        query_timezone=q.query_timezone,
    )

def run_look(look_id, filters):
    query = get_look_query(int(look_id))
    results = run_query_with_filter(query, filters)
    return results

def send_look_to_s3(look_id, filters, bucket_name):
    results = run_look(look_id, filters)
    put_object_s3(bucket_name, results)

def execute_action(r):
    if r.path == '/action_list':
      return action_list(r)
    elif r.path == '/action_form':
      return action_form(r)
    else:
      filters = []
      bucket_name = 'looker-bryce'
      send_look_to_s3(look_id=786, filters=filters, bucket_name=bucket_name)

    return r_handle(200, "ok")
