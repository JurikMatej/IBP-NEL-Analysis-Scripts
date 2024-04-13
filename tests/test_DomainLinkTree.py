import pytest
from src.classes.DomainLinkTree import DomainLinkTree, LinkNode, StructureNode


class TestDomainLinkRegistry:
    
    def test_init__simple_level_1_links(self):
        input_links = [
            'about',
            'events',
            'news'
        ]
        link_registry = DomainLinkTree("test.com", input_links)
        domain_node = link_registry.get_root()

        assert domain_node == LinkNode('')
        assert len(domain_node.children) == 3

        about_node = domain_node.get_child_by_label('about')
        events_node = domain_node.get_child_by_label('events')
        news_node = domain_node.get_child_by_label('news')

        assert about_node == LinkNode('about')
        assert events_node == LinkNode('events')
        assert news_node == LinkNode('news')

    @pytest.mark.parametrize(
        "init_links, input_links",
        [
            (
                [
                    'sub/page/this/must_stay_in_the_tree'
                ],
                [
                    'sub/page/this',
                    'sub/page/this/and_this_will_not_delete_the_first_too',
                ],
            )
        ]
    )
    def test_add__added_links_do_not_override_existing_structure(self, init_links, input_links):
        """
        Expected tree registry:

        Link('')
        [
            Structure('sub')
            [
                Structure('page')
                [
                    Link('this')
                    [
                        Link('must_stay_in_the_tree')
                        Link('and_this_will_not_delete_the_first_too')
                    ]
                ]
            ]
        ]
        """
        link_registry = DomainLinkTree("test.com", init_links)
        link_registry.add(input_links)

        domain_node = link_registry.get_root()

        assert domain_node == LinkNode('')
        assert len(domain_node.children) == 1

        sub_node = domain_node.get_child_by_label('sub')
        assert sub_node == StructureNode('sub')
        assert len(sub_node.children) == 1

        page_node = sub_node.get_child_by_label('page')
        assert page_node == StructureNode('page')
        assert len(page_node.children) == 1

        this_node = page_node.get_child_by_label('this')
        assert this_node == LinkNode('this')
        assert len(this_node.children) == 2

        assert this_node.get_child_by_label('must_stay_in_the_tree') == LinkNode('must_stay_in_the_tree')
        assert this_node.get_child_by_label('and_this_will_not_delete_the_first_too') == \
               LinkNode('and_this_will_not_delete_the_first_too')

    def test_init__complex_init(self):
        """
        Expected tree registry:

        Link('')
        [
            Link('about')
            Link('events')
            [
                Link('123')
                Link('XYZ')
                [
                    Link('ABC')
                    [
                        Link('WWW')
                    ]
                ]
            ]
            Link('news')
            [
                Link('grand')
            ]
        ]
        """
        input_links = [
            'about',
            'events',
            'events/123',
            'events/XYZ',
            'events/XYZ/ABC',
            'events/XYZ/ABC/WWW',
            'news',
            'news/grand'
        ]
        link_registry = DomainLinkTree("test.com", input_links)

        domain_node = link_registry.get_root()

        assert isinstance(domain_node, LinkNode)
        assert domain_node.label == ''
        assert len(domain_node.children) == 3

        # LINK GROUP - /about
        about_node = domain_node.get_child_by_label('about')
        assert isinstance(about_node, LinkNode)
        assert about_node.label == 'about'
        assert len(about_node.children) == 0

        # LINK GROUP - /events
        events_node = domain_node.get_child_by_label('events')
        assert isinstance(events_node, LinkNode)
        assert events_node.label == 'events'
        assert len(events_node.children) == 2

        events_123_node = events_node.get_child_by_label('123')
        assert isinstance(events_123_node, LinkNode)
        assert events_123_node.label == '123'
        assert len(events_123_node.children) == 0

        events_xyz_node = events_node.get_child_by_label('XYZ')
        assert isinstance(events_xyz_node, LinkNode)
        assert events_xyz_node.label == 'XYZ'
        assert len(events_xyz_node.children) == 1

        events_xyz_abc = events_xyz_node.get_child_by_label('ABC')
        assert isinstance(events_xyz_abc, LinkNode)
        assert events_xyz_abc.label == 'ABC'
        assert len(events_xyz_abc.children) == 1

        events_xyz_abc_www_node = events_xyz_abc.get_child_by_label('WWW')
        assert isinstance(events_xyz_abc_www_node, LinkNode)
        assert events_xyz_abc_www_node.label == 'WWW'
        assert len(events_xyz_abc_www_node.children) == 0

        # LINK GROUP - /news
        news_node = domain_node.get_child_by_label('news')
        assert isinstance(news_node, LinkNode)
        assert news_node.label == 'news'
        assert len(news_node.children) == 1

        news_grand_node = news_node.get_child_by_label('grand')
        assert isinstance(news_grand_node, LinkNode)
        assert news_grand_node.label == 'grand'
        assert len(news_grand_node.children) == 0

    def test_get_next__links_traversed_breadth_first_by_label_level(self):
        input_domain = "test.com"
        input_links = [
            'about',
            'events',
            'events/123',
            'events/XYZ',
            'events/XYZ/ABC',
            'news',
            'news/grand'
        ]
        expected_link_order = [
            "https://test.com",
            "https://test.com/about",
            "https://test.com/events",
            "https://test.com/news",
            "https://test.com/events/123",
            "https://test.com/events/XYZ",
            "https://test.com/news/grand",
            "https://test.com/events/XYZ/ABC",
        ]

        link_registry = DomainLinkTree(input_domain, input_links)

        link_order = []
        next_link = link_registry.get_next()
        while next_link is not None:
            link_order.append(next_link)
            next_link = link_registry.get_next()

        assert link_order == expected_link_order

    @pytest.mark.parametrize(
        "input_domain, input_link, expected_node_converted_link",
        [
            ("test.com", "",                    "https://test.com"),
            ("test.com", "events",              "https://test.com/events"),
            ("test.com", "events/XYZ",          "https://test.com/events/XYZ"),
            ("test.com", "events/XYZ/ABC",      "https://test.com/events/XYZ/ABC"),
            ("test.com", "events/XYZ/ABC/WWW",  "https://test.com/events/XYZ/ABC/WWW"),
        ]
    )
    def test_node_to_link(self, input_domain, input_link, expected_node_converted_link):
        link_registry = DomainLinkTree(input_domain, [input_link])

        link_level_labels = input_link.split('/')
        target_node = link_registry.get_root()
        for link_level_label in link_level_labels:
            target_node = target_node.get_child_by_label(link_level_label)

        assert link_registry.node_to_link(target_node) == expected_node_converted_link
