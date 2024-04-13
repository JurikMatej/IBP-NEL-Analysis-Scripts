import pytest
from src.classes.DomainLinkRegistry import DomainLinkRegistry, LinkNode, StructureNode


class TestDomainLinkRegistry:
    
    def test_init__simple_level_1_links(self):
        input_links = [
            'about',
            'events',
            'news'
        ]
        link_registry = DomainLinkRegistry("test.com", input_links)
        domain_node = link_registry.get_link_tree()

        assert domain_node == LinkNode('')
        assert len(domain_node.children) == 3

        about_node = domain_node.get_child_by_name('about')
        events_node = domain_node.get_child_by_name('events')
        news_node = domain_node.get_child_by_name('news')

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
        link_registry = DomainLinkRegistry("test.com", init_links)
        link_registry.add(input_links)

        domain_node = link_registry.get_link_tree()

        assert domain_node == LinkNode('')
        assert len(domain_node.children) == 1

        sub_node = domain_node.get_child_by_name('sub')
        assert sub_node == StructureNode('sub')
        assert len(sub_node.children) == 1

        page_node = sub_node.get_child_by_name('page')
        assert page_node == StructureNode('page')
        assert len(page_node.children) == 1

        this_node = page_node.get_child_by_name('this')
        assert this_node == LinkNode('this')
        assert len(this_node.children) == 2

        assert this_node.get_child_by_name('must_stay_in_the_tree') == LinkNode('must_stay_in_the_tree')
        assert this_node.get_child_by_name('and_this_will_not_delete_the_first_too') == \
               LinkNode('and_this_will_not_delete_the_first_too')

    @pytest.mark.parametrize(
        "init_links",
        [
            (
                [
                    'about',
                    'events',
                    'events/123',
                    'events/XYZ',
                    'events/XYZ/ABC',
                    'events/XYZ/ABC/WWW',
                    'news',
                    'news/grand'
                ]
            )
        ]
    )
    def test_init__complex_init(self, init_links):
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
        link_registry = DomainLinkRegistry("test.com", init_links)

        domain_node = link_registry.get_link_tree()

        assert domain_node == LinkNode('')
        assert len(domain_node.children) == 3

        # LINK GROUP - /about
        about_node = domain_node.get_child_by_name('about')
        assert about_node == LinkNode('about')
        assert len(about_node.children) == 0

        # LINK GROUP - /events
        events_node = domain_node.get_child_by_name('events')
        assert events_node == LinkNode('events')
        assert len(events_node.children) == 2

        events_123_node = events_node.get_child_by_name('123')
        assert events_123_node == LinkNode('123')
        assert len(events_123_node.children) == 0

        events_xyz_node = events_node.get_child_by_name('XYZ')
        assert events_xyz_node == LinkNode('XYZ')
        assert len(events_xyz_node.children) == 1

        events_xyz_abc = events_xyz_node.get_child_by_name('ABC')
        assert events_xyz_abc == LinkNode('ABC')
        assert len(events_xyz_abc.children) == 1

        events_xyz_abc_www_node = events_xyz_abc.get_child_by_name('WWW')
        assert events_xyz_abc_www_node == LinkNode('WWW')
        assert len(events_xyz_abc_www_node.children) == 0

        # LINK GROUP - /news
        news_node = domain_node.get_child_by_name('news')
        assert news_node == LinkNode('news')
        assert len(news_node.children) == 1

        news_grand_node = news_node.get_child_by_name('grand')
        assert news_grand_node == LinkNode('grand')
        assert len(news_grand_node.children) == 0
