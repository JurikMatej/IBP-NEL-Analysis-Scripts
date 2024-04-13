from __future__ import annotations

from abc import ABC
from collections import deque
from typing import List, Any


class Node(ABC):

    def __init__(self, label: str):
        self.label = label

        self.parent: Node | None = None
        self.children: List[Node] = []

    def __eq__(self, other: Node):
        """Object identity (used to check for equality when looking up a node in another node's child list)"""
        return self.label == other.label

    def get_child_by_label(self, label: str, default: Any = None) -> Node:
        for child in self.children:
            if child.label == label:
                return child
        return default

    def add_child(self, child: Node):
        if child not in self.children:
            child.parent = self
            self.children.append(child)

    def remove_child(self, child: Node):
        self.children.remove(child)


class StructureNode(Node):
    """Node used to mark link tree structure, not a usable link"""

    def __init__(self, label: str):
        super().__init__(label)

    def __repr__(self):
        return f"<StructureNode label='{self.label}'>"


class LinkNode(Node):
    """Node used to mark usable links"""

    def __init__(self, label: str):
        super().__init__(label)

        # Additional visited field
        self.visited: bool = False

    def __repr__(self):
        return f"<LinkNode label='{self.label}' visited='{self.visited}'>"


class DomainLinkTree:
    """
    A "registry" to manage URL links.
    Supports adding new links to a tree structure and retrieving them using breadth-first traversal strategy.
    When a link is retrieved, it will be internally marked as visited, so it does not have to be visited again.
    """

    def __init__(self, domain_name: str, initial_links: List[str] = None):
        self._domain_name = domain_name

        # Here, the LinkNode is a link to the base domain (http://domain_name/)
        self._link_tree_root: LinkNode = LinkNode('')

        if initial_links:
            self.add(initial_links)

    def get_next(self):
        traverse_queue = deque()

        # Verify root
        if not self._link_tree_root.visited:
            self._link_tree_root.visited = True
            return self.node_to_link(self._link_tree_root)

        # Expand root's children
        current_node = self._link_tree_root
        for child in current_node.children:
            traverse_queue.appendleft(child)

        while len(traverse_queue) > 0:
            # Get and verify current
            current_node = traverse_queue.pop()
            if isinstance(current_node, LinkNode) and current_node.visited is False:
                # Return the node if it is an unvisited LinkNode
                current_node.visited = True
                return self.node_to_link(current_node)

            # Expand current's children
            for child in current_node.children:
                traverse_queue.appendleft(child)

        # No unvisited links available
        return None

    def node_to_link(self, node: LinkNode):
        if node is None:
            return f"https://{self._domain_name}"

        result = node.label
        parent_node = node.parent
        while parent_node is not None:
            if parent_node.label != '':
                result = f"{parent_node.label}/{result}"
            parent_node = parent_node.parent

        if result.strip() == "":
            return f"https://{self._domain_name}"
        return f"https://{self._domain_name}/{result}"

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
                    # Any other part is just a placeholder StructureNode with a label
                    node_to_add = StructureNode(current_part)

                # Insert or update the node into the tree
                if node_to_add not in parent_node.children:
                    # If not created yet - simply insert
                    parent_node.add_child(node_to_add)

                else:
                    # If created already - update it only when adding the last part of a link (change Structure to Link)
                    existing_current_part_node = parent_node.get_child_by_label(current_part)
                    if isinstance(node_to_add, LinkNode) and isinstance(existing_current_part_node, StructureNode):
                        # Adding the last part - replace the existing Structure Node with a Link Node (keep children)
                        existing_current_part_node_children = existing_current_part_node.children

                        node_to_add.children = existing_current_part_node_children
                        parent_node.remove_child(existing_current_part_node)
                        parent_node.add_child(node_to_add)

                # Traverse down the tree to the node that was created
                parent_node = parent_node.get_child_by_label(current_part)

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
    #                      .get_child_by_label(current_level)
    #                      .add_child(StructureNode(next_level_to_add)))
    #
    #                 else:
    #                     # Child node update
    #
    #                     # Traverse by levels stored in traverse stack
    #                     sub_node_to_update = self._link_tree_root.get_child_by_label(current_link_level_stack[0])
    #                     for existing_link in current_link_level_stack[1:-1]:
    #                         next_traverse_node = sub_node_to_update.get_child_by_label(existing_link)
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
    #                     if sub_node_to_update.get_child_by_label(last_existing_level) is None:
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

    def get_root(self):
        return self._link_tree_root
