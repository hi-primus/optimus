# Reference  https://vega.github.io/vega-lite/docs/bin.html#binned
ALTAIR_HIST_TEMPLATE = {
    "$schema": "https://vega.github.io/schema/vega-lite/v3.json",
    "data": {
        "values": []
    },
    "mark": "bar",
    "encoding": {
        "x": {
            "title": "bins",
            "field": "bin_start",
            "bin": {
                "binned": True,
                "step": 1
            },
            "type": "quantitative"
        },
        "x2": {"field": "bin_end"},
        "y": {
            "title": "count",
            "field": "count",
            "type": "quantitative"
        },
        "tooltip": [
            {"type": "nominal", "field": "count"},
            {"type": "nominal", "field": "bin_start"},
            {"type": "nominal", "field": "bin_end"}
        ]
    }
}

ALTAIR_BAR_TEMPLATE = {
    "$schema": "https://vega.github.io/schema/vega-lite/v3.json",
    "data": {
        "values": []
    },
    "mark": "bar",
    "encoding": {
        "x": {"type": "nominal", "field": "value"},
        "y": {"type": "quantitative", "field": "count"},
        "tooltip": [
            {"type": "nominal", "field": "value"},
            {"type": "nominal", "field": "count"}
        ]
    }
}
