from typing import List

import networkx as nx

from src.db_constructs.DBObject import DBObject


def calc_fill_order(db_objects: List[DBObject], ordered_list=None):
    # Create relation graph
    relation_graph = nx.DiGraph()
    for db_object in db_objects:
        for db_relation in db_object.relations:
            origin_index = next((i for i, obj in enumerate(db_objects) if obj.name == db_relation.origin_table))
            target_index = next((i for i, obj in enumerate(db_objects) if obj.name == db_relation.target_table))
            relation_graph.add_edge(origin_index, target_index)

    # Get strongly connected components
    scc_indexes = list(nx.strongly_connected_components(relation_graph))
    scc_order = list(nx.topological_sort(nx.condensation(relation_graph)))
    scc_order.reverse()

    if ordered_list is None:
        ordered_list = []

    for i in scc_order:
        scc_index_list = list(scc_indexes[i])
        if len(scc_index_list) == 1:
            ordered_list.append(db_objects[scc_index_list[0]])
        else:
            scc_db_objects = [db_objects[j] for j in scc_index_list]
            # mark a table as half filled if its relations are nullable
            # then try to find a sub-order
            for scc_db_object in scc_db_objects:
                for object_relation in scc_db_object.relations:
                    if not [att for att in scc_db_object.attributes if att.name == object_relation.origin_name][0].nullable:
                        break
                else:
                    ordered_list.append(scc_db_object)
                    scc_db_objects.remove(scc_db_object)
                    calc_fill_order(scc_db_objects, ordered_list)

    return ordered_list
