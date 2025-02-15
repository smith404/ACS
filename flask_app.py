# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from flask import Flask, send_from_directory, render_template, Response, request
from flask_wtf.csrf import CSRFProtect
from pattern import Pattern, PatternEvaluator, PatternSlice

app = Flask(__name__)
csrf = CSRFProtect(app)

@app.route('/')
def home() -> str:
    return "Index Page"

@app.route('/pattern/')
@app.route('/pattern/<name>')
def pattern(name: str = None) -> str:
    return render_template('pattern_test.html', pattern=name)

@app.route('/desktop/')
def desktop() -> str:
    return render_template('desktop.html')

@app.route('/pattern/svg/<name>')
def pattern_svg(name: str) -> Response:
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    svg_type = request.args.get('type', 'full')
    latest_written_slice = int(request.args.get('lw', 0))
    svg_content = generate_svg_content(evaluator, svg_type, latest_written_slice)
    return Response(svg_content, mimetype='image/svg+xml')

@app.route('/pattern/new')
def pattern_new() -> Response:
    pattern = Pattern()
    return Response(pattern.to_json(), mimetype='application/json')

@app.route('/pattern/load/<name>')
def pattern_object(name: str) -> Response:
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    return Response(pattern.to_json(), mimetype='application/json')

@app.route('/pattern/save', methods=['POST'])
def save_pattern() -> Response:
    data = request.json
    if not data:
        return Response("Invalid data", status=400)
    
    pattern = Pattern.from_json(data)
    pattern.save_to_file(f'scratch/{pattern.identifier}.json')
    return Response("Pattern saved successfully", status=200)

def generate_svg_content(evaluator: PatternEvaluator, svg_type: str, latest_written_slice: int) -> str:
    if svg_type == 'written':
        return evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice=latest_written_slice, pre_colour='blue', colour='white')
    elif svg_type == 'unwritten':
        return evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice=latest_written_slice)
    elif svg_type == 'lic':
        time_point = evaluator.get_earliest_end_point_of_slice(latest_written_slice)
        return evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice=latest_written_slice, day_cut=time_point, pre_colour='blue', colour='white', condition="and")
    elif svg_type == 'lrc':
        time_point = evaluator.get_earliest_end_point_of_slice(latest_written_slice)
        return evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice=latest_written_slice, day_cut=time_point, pre_colour='white', colour='blue', condition="and")
    elif svg_type == 'upr':
        time_point = evaluator.get_earliest_end_point_of_slice(latest_written_slice)
        return evaluator.create_svg(evaluator.pattern_blocks, day_cut=time_point)
    else:
        return evaluator.create_svg(evaluator.pattern_blocks)

@app.route('/static/<path:filename>')
def static_files(filename: str) -> Response:
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True)