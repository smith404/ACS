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
from pattern import Pattern, PatternEvaluator

app = Flask(__name__)
csrf = CSRFProtect(app)

@app.route('/')
def home():
    return "Index Page"

@app.route('/pattern/')
@app.route('/pattern/<name>')
def pattern(name=None):
    return render_template('pattern_test.html', pattern=name)

@app.route('/desktop/')
def desktop():
    return render_template('desktop.html')

@app.route('/pattern/svg/<name>')
def pattern_svg(name):
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    svg_type = request.args.get('type', 'full')
    latest_written_slice = int(request.args.get('lw', 0))
    print(latest_written_slice)
    if svg_type == 'lic':
        time_point = evaluator.get_earliest_end_point_of_slice(latest_written_slice)
        svg_content = evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice = latest_written_slice, day_cut = time_point)
    elif svg_type == 'lrc':
        time_point = evaluator.get_earliest_end_point_of_slice(latest_written_slice)
        svg_content = evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice = latest_written_slice, day_cut = time_point, pre_colour='blue', colour='white')
    elif svg_type == 'upr':
        time_point = evaluator.get_earliest_end_point_of_slice(latest_written_slice)
        svg_content = evaluator.create_svg(evaluator.pattern_blocks, day_cut = time_point)
    elif svg_type == 'written':
        svg_content = evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice=latest_written_slice, pre_colour='blue', colour='white')
    elif svg_type == 'unwritten':
        svg_content = evaluator.create_svg(evaluator.pattern_blocks, latest_written_slice=latest_written_slice)
    else:
        svg_content = evaluator.create_svg(evaluator.pattern_blocks)
    return Response(svg_content, mimetype='image/svg+xml')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True)