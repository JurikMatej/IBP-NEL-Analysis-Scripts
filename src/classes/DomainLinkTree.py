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

        self._visited_links_count: int = 0

        # Here, the LinkNode translates to the base domain link - https://{domain_name}
        self._link_tree_root: LinkNode = LinkNode('')

        if initial_links:
            self.add(initial_links)

    def __repr__(self):
        return f"<DomainLinkTree domain='{self._domain_name}' visited_links_count='{self._visited_links_count}'>"

    def get_next(self):
        traverse_queue = deque()

        # Verify root
        if not self._link_tree_root.visited:
            self._link_tree_root.visited = True
            self._visited_links_count += 1
            return self._node_to_link(self._link_tree_root)

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
                self._visited_links_count += 1
                return self._node_to_link(current_node)

            # Expand current's children
            for child in current_node.children:
                traverse_queue.appendleft(child)

        # No unvisited links available
        return None

    def _node_to_link(self, node: LinkNode):
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

    def get_visited_links_count(self):
        return self._visited_links_count

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

    def get_root(self):
        return self._link_tree_root
