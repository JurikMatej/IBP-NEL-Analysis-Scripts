from __future__ import annotations
from typing import List, Any
from abc import ABC


class Node(ABC):

    def __init__(self, name: str):
        self.name = name
        self.children: List[Node] = []

    def __eq__(self, other):
        """Object identity (used to check for equality when looking up a node in another node's child list)"""
        return self.name == other.name

    def get_child_by_name(self, name: str, default: Any = None) -> Node:
        for child in self.children:
            if child.name == name:
                return child
        return default

    def add_child(self, child: Node):
        if child not in self.children:
            self.children.append(child)

    def remove_child(self, child: Node):
        self.children.remove(child)


class StructureNode(Node):
    """Node used to mark link structure, not a usable link"""

    def __init__(self, name: str):
        super().__init__(name)
        self.children: List[Node] = []


class LinkNode(Node):
    """Node used to mark usable links"""

    def __init__(self, name: str):
        super().__init__(name)
        self.visited = False
        self.children: List[Node] = []


class DomainLinkRegistry:
    """
    A registry to manage URL links.
    Supports adding new links to a tree structure and retrieving them using breadth-first traversal strategy.
    When a link is retrieved, it will be internally marked as visited, so it does not have to be visited again.
    """

    def __init__(self, domain_name: str, initial_links: List[str] = None):
        self._domain_name = domain_name

        # Here, the LinkNode is a link to the base domain (http://domain_name/)
        self._link_tree_root: Node = LinkNode('')

        if initial_links:
            self.add(initial_links)

    def get_next(self):
        pass

    def add(self, new_links: List[str]):
        for current_link in new_links:
            current_link_parts = current_link.split('/')
            parent_node = self._link_tree_root

            for current_part_idx, current_part in enumerate(current_link_parts):
                
                adding_last_part = current_part_idx == len(current_link_parts) - 1 

                # Init the node
                if adding_last_part:
                    # The LAST part of any link marks a whole LINK
                    node_to_add = LinkNode(current_part)
                else:
                    # Any other part is just a placeholder StructureNode with a name
                    node_to_add = StructureNode(current_part)

                # Insert or update the node into the tree
                if node_to_add not in parent_node.children:
                    # If not created yet - simply insert
                    parent_node.add_child(node_to_add)

                else:
                    # If created already - update it only when adding the last part of a link (change Structure to Link)
                    existing_current_part_node = parent_node.get_child_by_name(current_part)
                    if isinstance(node_to_add, LinkNode) and isinstance(existing_current_part_node, StructureNode):
                        # Adding the last part - replace the existing Structure Node with a Link Node (keep children)
                        existing_current_part_node_children = existing_current_part_node.children

                        node_to_add.children = existing_current_part_node_children
                        parent_node.remove_child(existing_current_part_node)
                        parent_node.add_child(node_to_add)

                # Traverse down the tree to the node that was created
                parent_node = parent_node.get_child_by_name(current_part)

    # def add(self, new_links: List[str]):
    #     current_link_level_stack = []
    #
    #     to_organize = sorted(list(set(new_links)))
    #     for link in to_organize:
    #         link_levels = link.split('/')
    #         link_levels.extend([None])  # Terminate level sequence with None literal
    #
    #         for idx, current_level in enumerate(link_levels):
    #             next_level_to_add = link_levels[idx + 1]
    #
    #             if next_level_to_add is not None:
    #                 # Add next level
    #                 current_link_level_stack.append(current_level)
    #
    #                 if idx == 0:
    #                     # Tree root update
    #
    #                     # subtree_to_update = self._link_tree.get(current_level, None)
    #                     # if subtree_to_update is None:
    #
    #                     current_level_node = StructureNode(current_level)
    #                     if current_level_node not in self._link_tree_root.children:
    #                         # self._link_tree[current_level] = {
    #                         #     # When there is still next_level_to_add, use StructureNodes to stub structure values
    #                         #     next_level_to_add: StructureNode()
    #                         # }
    #
    #                         # When there is still next_level_to_add, use StructureNodes to stub structure values
    #                         self._link_tree_root.add_child(current_level_node)
    #
    #                     (self._link_tree_root
    #                      .get_child_by_name(current_level)
    #                      .add_child(StructureNode(next_level_to_add)))
    #
    #                 else:
    #                     # Child node update
    #
    #                     # Traverse by levels stored in traverse stack
    #                     sub_node_to_update = self._link_tree_root.get_child_by_name(current_link_level_stack[0])
    #                     for existing_link in current_link_level_stack[1:-1]:
    #                         next_traverse_node = sub_node_to_update.get_child_by_name(existing_link)
    #
    #                     # Traverse by levels stored in traverse stack
    #                     # subtree_to_update = self._link_tree_root[current_link_level_stack[0]]  # (tree) root level
    #                     # for existing_level in current_link_level_stack[1:-1]:
    #                     #     next_traverse_level = subtree_to_update[existing_level]
    #                     #     # Traverse deeper only until a placeholder node is found - StructureNode
    #                     #     if not isinstance(next_traverse_level, StructureNode):
    #                     #         subtree_to_update = next_traverse_level
    #
    #                     last_existing_level = current_link_level_stack[-1]
    #
    #                     if sub_node_to_update.get_child_by_name(last_existing_level) is None:
    #                         new_node = StructureNode(last_existing_level)
    #                         sub_node_to_update.add_child(new_node)
    #
    #
    #                     if isinstance(subtree_to_update[last_existing_level], StructureNode):
    #                         # Replace StructureNode stub with new nested tree
    #                         subtree_to_update[last_existing_level] = {
    #                             next_level_to_add: StructureNode()
    #                         }
    #                     else:
    #                         if next_level_to_add not in subtree_to_update[last_existing_level]:
    #                             subtree_to_update[last_existing_level][next_level_to_add] = StructureNode()
    #
    #             elif next_level_to_add is None and idx != 0:
    #                 # No 'next level' & Current level was already added in previous iteration
    #
    #                 current_link_level_stack.clear()
    #                 break
    #             else:
    #                 # This is the root level, but no more levels available
    #                 # Add this one as a LINK Node
    #                 self._link_tree_root.add_child(LinkNode(current_level))
    #                 # Reset stack and break
    #                 current_link_level_stack.clear()
    #                 break

    def get_link_tree(self):
        return self._link_tree_root
