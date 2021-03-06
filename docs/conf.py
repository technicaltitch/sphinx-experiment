#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys

from recommonmark.parser import CommonMarkParser
from recommonmark.transform import AutoStructify


from unittest.mock import MagicMock

class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, name):
        return MagicMock()

MOCK_MODULES = ['gdal', 'pandas', 'geopandas', 'matplotlib', 'pytables']
sys.modules.update((mod_name, Mock()) for mod_name in MOCK_MODULES)


# Pipelines documentation build configuration file, created by
# sphinx-quickstart on Tue Dec 19 21:33:10 2017.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
sys.path.insert(0, os.path.abspath('..'))


# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.imgmath',
    'sphinx.ext.graphviz',
    'sphinx.ext.inheritance_diagram',
    'pipelines.docs.ext.pipeline_diagram',
    'sphinx.ext.napoleon',  # only needed if we go for Google or Numpy docstring formats
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
source_suffix = ['.rst', '.md']

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'sphinx-experiment'
copyright = '2018, Chris'
author = 'Chris'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = ''
# The full version, including alpha/beta/rc tags.
release = ''

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

project = 'RM'
project_description = 'RM Pipeline docs'

copyright = '2018, Kimetrica llc'  # NOQA
author = 'Kimetrica llc'


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'default'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    'description': project_description
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# This is required for the alabaster theme
# refs: http://alabaster.readthedocs.io/en/latest/installation.html#sidebars
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',  # needs 'show_related': True theme option to display
        'searchbox.html',
    ]
}

html_logo = '_static/logo.png'

# -- Options for HTMLHelp output ------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'sphinx-experimentdoc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    # 'papersize': 'letterpaper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    # 'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',

    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
  ('index', 'ReadtheDocsTemplate.tex', u'Read the Docs Template Documentation',
   u'Read the Docs', 'manual'),
]


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'sphinx-experiment', 'sphinx-experiment Documentation',
     [author], 1)
]

# Napoleon settings (all their default settings at the moment)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True

# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'sphinx-experiment', 'sphinx-experiment Documentation',
     author, 'sphinx-experiment', 'One line description of project.',
     'Miscellaneous'),
]

# Breaks diagram generation: autodoc_inherit_docstrings = False

# Global substituions
rst_epilog = """
.. |ProjectName| replace:: {project}
""".format(
    project=project,
)

# def process_docstring(app, what, name, obj, options, lines):
#     lines.extend(['        Quick check to confirm napoleon is compatible with graphviz:', '',
#                   '        .. graphviz::', '',
#                   '             digraph example {',
#                   '                 a [label="chris_pipeline.analysis.AppendDataFrames", href="chris_pipeline.html#chris_pipeline.analysis.AppendDataFrames", target="_blank"];',
#                   '                 b [label="other"];',
#                   '                 a -> b;',
#                   '             }', '', ])

def setup(app):
    app.add_config_value('recommonmark_config',
                         {'auto_toc_tree_section': False,
                          'graphviz_output_format': 'svg',
                          },
                         True)
    app.add_transform(AutoStructify)
#    app.connect('autodoc-process-docstring', process_docstring)


