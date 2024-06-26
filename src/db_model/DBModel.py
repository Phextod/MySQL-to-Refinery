import inspect
import math
from typing import Dict
from jinja2 import Template

from src.db_model.DBAttribute import DBAttribute
from src.db_model.DBRelation import DBRelation
from src.db_model.DBTable import DBTable, FillStatus

from src.utility import CalcFillOrder


class DBModel:
    def __init__(self, db_tables=None):
        self.db_tables: Dict[str, DBTable] = db_tables or {}

    def add_table(self, table_name, db_table):
        self.db_tables.update({table_name: db_table})

    def generate_refinery_code(self, node_multiplicity_multiplier, multiplicity_max_multiplier):
        template = Template(inspect.cleandoc("""
        {% for tab in db_tables.values() -%}
        class {{ tab.name }} {
        {%- if tab.relations -%}
            {%- for relation in tab.relations %}
            {{ relation.target_table_name }}
            {%- if relation.multiplicity_min == relation.multiplicity_max -%}
            [{{ relation.multiplicity_min }}]{{" "}}
            {%- else -%}
            [{{ relation.multiplicity_min }}..{{ relation.multiplicity_max }}]{{" "}}
            {%- endif -%}
            {{ relation.origin_attribute_name }}
            {%- endfor %}
        }
        {%- else -%}
        }
        {%- endif %}
        {% endfor %}
        //composite key constraints
        {% for table_name, keys in composite_keys.items() -%}
        error sameKeysFor_{{table_name}}({{table_name}} {{table_name}}_object) <-> 
            {{table_name}}_object != {{table_name}}_other,
            {% for key in keys -%}
            {{key}}({{table_name}}_object,ref_{{key}}),
            {{key}}({{table_name}}_other,ref_{{key}}){{ "," if not loop.last else "." }}
            {% endfor %}
        {% endfor %}
        scope node={{ node_multiplicity_min }}..{{ node_multiplicity_max }},
           {% for name, tab in db_tables.items() -%}
           {{ name }}={{ (tab.scope_count_min * scope_multi)|int }}..{{ tab.scope_count_max }}{{ "," if not loop.last else "." }}
           {% endfor %}
        """))

        composite_keys = self.get_reference_composite_keys()

        node_multiplicity_min = \
            math.ceil(sum(tab.scope_count_min for tab in self.db_tables.values()) * node_multiplicity_multiplier)
        node_multiplicity_max = math.ceil(node_multiplicity_min * multiplicity_max_multiplier)

        rendered_template = template.render(
            db_tables=self.db_tables,
            node_multiplicity_min=node_multiplicity_min,
            node_multiplicity_max=node_multiplicity_max,
            scope_multi=node_multiplicity_multiplier,
            composite_keys=composite_keys,
        )

        return rendered_template

    def refinery_result_to_sql(self, refinery_result_path):
        self.db_tables = CalcFillOrder.calc_fill_order(self.db_tables)
        self.refinery_result_to_db_objects(refinery_result_path)
        return self.db_objects_to_sql()

    def refinery_result_to_db_objects(self, refinery_result_path):
        with open(refinery_result_path, "r") as file:
            line = file.readline()

            # ignore lines until "declare"
            while not line.strip().startswith("declare"):
                line = file.readline()

            clean_line = line.replace("declare", "").strip()[:-1]
            object_names_and_tables = {name: "" for name in clean_line.split(", ")}
            line = file.readline()

            # ignore lines until object creation
            while line.strip().startswith("!exists"):
                line = file.readline()

            # create objects
            while "" in object_names_and_tables.values():
                table_name, object_name = line.strip(").\n").split("(")
                self.db_tables[table_name].add_object(object_name)
                object_names_and_tables.update({object_name: table_name})
                line = file.readline()

            # create relations
            while line:
                if line.strip().startswith("default"):
                    line = file.readline()
                    continue
                origin_attribute_name, object_names = line.strip(").\n").split("::")[-1].split("(")
                origin_object_name, target_object_name = object_names.split(", ")

                origin_table_name = object_names_and_tables[origin_object_name]
                target_table_name = object_names_and_tables[target_object_name]

                target_object_id = self.db_tables[target_table_name].get_id_of(target_object_name)

                origin_object_attributes = self.db_tables[origin_table_name].objects[origin_object_name]
                origin_object_attributes.update({origin_attribute_name: target_object_id})

                line = file.readline()

    def db_objects_to_sql(self):

        # delete statements
        template_delete_from_tables = Template(inspect.cleandoc("""
        SET FOREIGN_KEY_CHECKS=0;
        {% for table_name in db_tables.keys() -%}
        DELETE FROM `{{ table_name }}`;
        {% endfor -%}
        SET FOREIGN_KEY_CHECKS=1;
        """))

        delete_from_tables = template_delete_from_tables.render(
            db_tables=self.db_tables,
        )

        template_insert_statements = Template(inspect.cleandoc("""
        {% if has_self_reference %}
        SET FOREIGN_KEY_CHECKS=0; {% endif %}
        INSERT INTO `{{db_table.name}}` (
        {%- for attribute in db_table.attributes -%}
        `{{attribute.name}}`{{ "," if not loop.last else "" }}
        {%- endfor -%}
        ) VALUES
        {% for object_name, db_object in db_table.objects.items() -%}
        (
        {%- for attribute in db_table.attributes -%}
        {%- if attribute.name in unfilled_reference_attribute_names -%}
        NULL
        {%- else -%}
        {{db_object[attribute.name]}}
        {%- endif -%}
        {{ "," if not loop.last else "" }}
        {%- endfor -%}
        ){{ "," if not loop.last else ";" }}
        {%- endfor %}
        {% if has_self_reference -%} SET FOREIGN_KEY_CHECKS=1;
        {% endif %}
        """))

        # insert statements
        insert_statements = ""
        for db_table in self.db_tables.values():
            has_self_reference = bool([r for r in db_table.relations if r.target_table_name == r.origin_table_name])

            unfilled_reference_attribute_names = []
            for relation in db_table.relations:
                # Self references do not matter
                if relation.target_table_name == db_table.name:
                    continue
                if self.db_tables[relation.target_table_name].fill_status == FillStatus.NOT_FILLED:
                    unfilled_reference_attribute_names.append(relation.origin_attribute_name)

            if unfilled_reference_attribute_names:
                db_table.fill_status = FillStatus.HALF_FILLED
            else:
                db_table.fill_status = FillStatus.FILLED

            insert_statements += template_insert_statements.render(
                db_table=db_table,
                has_self_reference=has_self_reference,
                unfilled_reference_attribute_names=unfilled_reference_attribute_names,
            )

        template_update_statements = Template(inspect.cleandoc(""" 
        {% for object_name, db_object in db_table.objects.items() -%}
        UPDATE `{{db_table.name}}`
        SET{{" "}}
        {%- for attribute_name in updatable_attribute_names -%}
        {%- if attribute_name in unfilled_reference_attribute_names -%}
         NULL
        {%- else -%}
         `{{attribute_name}}` = {{db_object[attribute_name]}}
        {%- endif -%}
        {{ ", " if not loop.last else "" }}
        {%- endfor %}
        WHERE{{" "}}
        {%- for attribute_name in id_attribute_names -%}
        `{{attribute_name}}` = {{db_object[attribute_name]}}{{ "," if not loop.last else ";" }}
        {% endfor %}
        {% endfor -%}
        """))

        # update statements
        update_statements = ""
        tables_to_update = [t for t in self.db_tables.values() if t.fill_status == FillStatus.HALF_FILLED]
        while tables_to_update:
            for db_table in tables_to_update:
                unfilled_reference_attribute_names = []
                updatable_attribute_names = []
                for relation in db_table.relations:
                    # Self references do not matter
                    if relation.target_table_name == db_table.name:
                        continue
                    if self.db_tables[relation.target_table_name].fill_status == FillStatus.NOT_FILLED:
                        unfilled_reference_attribute_names.append(relation.origin_attribute_name)
                    else:
                        updatable_attribute_names.append(relation.origin_attribute_name)

                id_attribute_names = [a.name for a in db_table.attributes if a.key_type == "PRI"]

                update_statements += template_update_statements.render(
                    db_table=db_table,
                    updatable_attribute_names=updatable_attribute_names,
                    unfilled_reference_attribute_names=unfilled_reference_attribute_names,
                    id_attribute_names=id_attribute_names,
                )

                if not unfilled_reference_attribute_names:
                    db_table.fill_status = FillStatus.FILLED
                    tables_to_update.remove(db_table)

        return f"{delete_from_tables}\n{insert_statements}\n{update_statements}"

    def get_reference_composite_keys(self):
        # if a key attribute is not a reference, then refinery can't generate (same) values for it
        reference_composite_keys = {}
        for db_table in self.db_tables.values():
            keys = [a.name for a in db_table.attributes if a.key_type == "PRI"]
            relation_attributes = [a.origin_attribute_name for a in db_table.relations]
            reference_keys = [k for k in keys if k in relation_attributes]
            if len(reference_keys) > 1:
                reference_composite_keys.update({db_table.name: reference_keys})
        return reference_composite_keys


def generate_db_model(tables, table_relations, table_descriptions) -> DBModel:
    """
    Generate DBModel with tables, relations, and attribute based on given schema data
    """
    db_model = DBModel()

    for table in tables:
        table_name = table[0]
        db_table = DBTable(table_name)

        # Adding attributes
        attributes_for_table = table_descriptions[table_name]
        for attribute in attributes_for_table:
            db_attribute = DBAttribute(attribute[0], attribute[1], attribute[2] == "YES",
                                       attribute[3], attribute[4], attribute[5])
            db_table.add_attribute(db_attribute)

        # Adding relations
        relations_for_table = [r for r in table_relations if r[1] == table_name]
        for relation in relations_for_table:
            db_relation = DBRelation(relation[1], relation[2], relation[4], relation[5], 1, 1)
            if [a for a in db_table.attributes if a.name == db_relation.origin_attribute_name][0].nullable:
                db_relation.multiplicity_min = 0
            db_table.add_relation(db_relation)

        db_model.add_table(table_name, db_table)

    return db_model
