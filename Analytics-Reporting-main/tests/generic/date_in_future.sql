{% test date_in_future(model,column_name)  %}
    select * 
    FROM {{model}}
    WHERE 
       {{column_name}} < CURRENT_DATE()

{% endtest %}


