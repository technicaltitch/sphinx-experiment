.. This is a comment

Welcome to sphinx-experiment's documentation!
=============================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Introduction
============

.. rubric:: Everything you need to know about |ProjectName|.

This is now tag 0.02

This documentation explains how analysis is performed for $ProjectName.

Additional assistance is available from the following sources:

* Link to `pipelines` repo Pages site from here? Or run all project docs from `pipelines`
repo? (`pipelines/docs/project.rst` references `../../docs/index.rst`). Currently I'll run in project repo
to simplify DAG generation bits. Can then revisit whether project `gitlab-ci.yml` needs its own Dockerfile
and bash scripts or whether we can centralize in `pipelines` repo with parameters.

More stuff

.. automodule:: chris_pipeline.analysis
    :members:

.. automodule:: chris_pipeline.analysis
    :members:
    :undoc-members:
    :show-inheritance:

.. include:: ../README.rst


* The :ref:`genindex`, :ref:`modindex` or the :doc:`detailed table of contents <contents>`.

* The Helpdesk via email or Redmine.

.. graphviz::

   digraph foo {
      "bar" -> "baz";
   }

.. graph:: foo

   "bar" -- "baz";

.. digraph:: foo

   "bar" -> "baz" -> "quux";

SVG permits URLs to be embedded in diagrams like so (click on the task node):

.. graphviz::

     digraph example {
         a [label="chris_pipeline.analysis.AppendDataFrames", href="chris_pipeline.html#chris_pipeline.analysis.AppendDataFrames", target="_blank"];
         b [label="other"];
         a -> b;
     }


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
