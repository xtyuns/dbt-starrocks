{% materialization upsert, adapter="starrocks" -%}

  -- exists output relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {% if existing_relation is none %}
    {{- exceptions.raise_compiler_error("output table not exists") -}}
  {% endif %}

  -- temporary relations
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {%- set preexisting_temp_relation = load_cached_relation(temp_relation)-%}
  {{ drop_relation_if_exists(preexisting_temp_relation) }}
  {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}

  {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {% set dest_cols = dest_cols_csv.split(', ') %}
  {% set temp_columns = adapter.get_columns_in_relation(temp_relation) %}
  {%- set temp_cols_csv = get_quoted_csv(temp_columns | map(attribute="name")) -%}
  {% set temp_cols = temp_cols_csv.split(', ') %}

  -- select columns
  {% set select_cols = [] %}
  {% for item in dest_cols %}
      {% if item in temp_cols %}
          {% do select_cols.append('a.' ~ item) %}
      {% else %}
          {% do select_cols.append('b.' ~ item) %}
      {% endif %}
  {% endfor %}
  {%- set select_cols_csv = select_cols | join(', ') %}

  {% call statement("main") %}
      insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ select_cols_csv }}
        from {{ temp_relation }} a left join {{ target_relation }} b using ({{ unique_key | join(', ') }})
    )
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
