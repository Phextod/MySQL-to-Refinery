from typing import Dict

import networkx as nx

from src.db_model.DBTable import DBTable


def calc_fill_order(db_tables: Dict[str, DBTable], ordered_dict=None):
    """
    Calculate the order in which the db tables should be filled in order not to violate foreign key constraints
    """
    # Create relation graph
    relation_graph = nx.DiGraph()
    for db_table in db_tables.values():
        for db_relation in db_table.relations:
            origin_table_name = db_relation.origin_table_name
            target_table_name = db_relation.target_table_name
            relation_graph.add_edge(origin_table_name, target_table_name)

    # Get strongly connected components
    scc_names = list(nx.strongly_connected_components(relation_graph))
    scc_order = list(nx.topological_sort(nx.condensation(relation_graph)))
    scc_order.reverse()

    if ordered_dict is None:
        ordered_dict = {}

    for i in scc_order:
        scc_name_list = list(scc_names[i])
        if len(scc_name_list) == 1:
            table_name = scc_name_list[0]
            ordered_dict.update({table_name: db_tables[table_name]})
        else:
            # TODO: test with reference loops
            scc_db_tables = [db_tables[name] for name in scc_name_list]
            # find a table that has a nullable relation
            # mark it, then try to find a sub-order
            for scc_db_table in scc_db_tables:
                for table_relation in scc_db_table.relations:
                    attribute = [att for att in scc_db_table.attributes
                                 if att.name == table_relation.origin_attribute_name][0]
                    if not attribute.nullable:
                        break
                else:
                    # TODO: mark for half filling
                    table_name = scc_db_table.name
                    ordered_dict.update({table_name: db_tables[table_name]})
                    scc_db_tables.remove(scc_db_table)
                    calc_fill_order({t.name: t for t in scc_db_tables}, ordered_dict)

    return ordered_dict
