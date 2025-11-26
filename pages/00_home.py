import solara


@solara.component
def Page():
    with solara.Column(align="center"):
        markdown = """
        Duckdb練習
        """

        solara.Markdown(markdown)
