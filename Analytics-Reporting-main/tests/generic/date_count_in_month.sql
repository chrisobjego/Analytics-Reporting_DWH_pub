{% test date_count_in_month(model,column_name)  %}
    select * 
    FROM {{model}}
    WHERE 
       {{column_name}} > 31

{% endtest %}

