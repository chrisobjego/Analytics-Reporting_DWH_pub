{% test date_is_past(model,column_name)  %}
    select * 
    FROM {{model}}
    WHERE 
       {{column_name}} > CURRENT_DATE()
{% endtest %}


