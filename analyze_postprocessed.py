"""
TODO
    1. Use the preprocessed data obtained from the output of preprocess_stored.py
    2. Create a set of functions computing the metrics agreed upon. These functions will create:
        a. Statements - describe a fact about NEL deployment (% of NEL deployments)
        b. Visualizations - additionally, if applicable, generate a graph displaying the metrics
                            in an easily understandable way
        c. Latex src text - if possible, also implement a latex file output capability that can than be leveraged
                            to save time when re-running the analysis later (save the statement and the visualization
                            as a latex file)
    A.
        The goal is to have the analysis located in one place (one runnable script) so that it is not so cumbersome to
        reproduce results or generate more results at a later time
        (in comparison to using the strategy to implement multiple jupyter notebooks)
    B.
        Keep in mind that this script should be doing as less as possible.
        Ideally, only load the data once (but it's a LOT of data)
        Ideally, compute all the metrics with it (implement the common structure well in the postprocess_stored.py)
        Ideally, output should be easily usable for the thesis - copy and paste .tex, or upload image to be used
            as a figure (this takes quite a lot of time and is not required by the analysis assignment)
"""