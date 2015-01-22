#!/usr/bin/env python

from lxml import etree
import MySQLdb
import string
import sys
import ConfigParser


class Hotcrp_paperlist_export:

    def __init__(self):
        self.config = self.get_config()
        con = self.get_db_con()
        self.papers = self.get_papers(con)
        self.xml_papers = etree.Element(self.config['output']['root_node'])

    @staticmethod
    def get_config(filename = "config.ini"):
        config = {}
    
        parser=ConfigParser.SafeConfigParser()
        parser.read([filename])

        for section in parser.sections():
            if section not in config.keys():
                config[section] = {}
            for key, value in parser.items(section):
                config[section][key] = value
        return config



    def get_db_con(self):
        con=MySQLdb.connect(passwd=self.config['hotcrp']['passwd'],
                            db=self.config['hotcrp']['db'],
                            user=self.config['hotcrp']['user'],
                            host=self.config['hotcrp']['host']
                        )
        return con

    def get_papers(self, con):
        query = "select paperId, title, authorInformation from Paper where outcome = 1 order by paperId"
        cur = con.cursor()
        cur.execute(query)
        papers = []
        for paperId, title, authorinformation in cur.fetchall():
            paper = {}
            paper['id'] = int(paperId)
            paper['title'] = self._fix_printable(title)
            paper['authorinformation'] = self._parse_authors(authorinformation)
            papers.append(paper)

        return papers

    def _fix_printable(self, culprit):
        
        clean_string = filter(lambda x: x in string.printable, culprit)

        if culprit != clean_string:
            sys.stderr.write("Warning: non-printable chars, replacing\n")
        return clean_string


    def _parse_authors(self, authorsinfo):
        authors = []
        for auth in authorsinfo.strip().split('\n'):
            author = {}
            author['fn'], author['ln'] = auth.split('\t')[0:2]
            
            author['fn'] = self._fix_printable(author['fn'])
            author['ln'] = self._fix_printable(author['ln'])

            authors.append(author)

        return authors

    def process_paper(self):

        for p in self.papers:
            paper = etree.Element("conference_paper")

            paper.attrib['publication_type'] = self.config['output']['publication_type']

            # Contributors
            contributors = etree.Element("contributors")

            first_author = True

            for author in p['authorinformation']:
                person_name = etree.Element("person_name")
                given_name = etree.Element("given_name")
                surname = etree.Element("surname")

                given_name.text = author['ln']
                surname.text = author['fn']

                if first_author:
                    person_name.attrib['sequence'] = "first"
                    first_author = False
                else:
                    person_name.attrib['sequence'] = "additional"

                person_name.attrib['contributor_role'] = "author"

                person_name.append(given_name)
                person_name.append(surname)

                contributors.append(person_name)

            # Titles
            titles = etree.Element("titles")
            title = etree.Element("title")
            title.text = p['title']

            titles.append(title)

            # Date
            publication_date = etree.Element("publication_date")
            year = etree.Element("year")
            year.text = self.config['output']['year']
            publication_date.append(year)

            # DOI
            doi_data = etree.Element("doi_data")
            doi = etree.Element("doi")
            resource = etree.Element("resource")

            doi.text = "{:}{:0>3d}".format(self.config['output']['doi_header'], p['id'])
            resource.text = self.config['output']['doi_resource_text']

            doi_data.append(doi)
            doi_data.append(resource)

            # Putting it together
            paper.append(contributors)
            paper.append(titles)
            paper.append(publication_date)
            paper.append(doi_data)

            self.xml_papers.append(paper)

    def output(self):
        return '<?xml version="1.0" encoding="UTF-8"?>\n{0}'.format(etree.tostring(self.xml_papers, pretty_print=True))

if __name__ == "__main__":
    h = Hotcrp_paperlist_export()
    h.process_paper()
    print h.output() ,

