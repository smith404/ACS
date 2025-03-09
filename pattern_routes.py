from flask import Blueprint, Response, request, render_template
from pattern import Pattern
from pattern_evaluator import PatternBlock, PatternEvaluator
from pattern_slice import PatternSlice

pattern_bp = Blueprint('pattern', __name__)

@pattern_bp.route('/pattern/new')
def pattern_new() -> Response:
    pattern = Pattern()
    slice = PatternSlice()
    slice.developmentPeriods = 1
    slice.weight = 1
    pattern.add_slice(slice)
    return Response(pattern.to_json(), mimetype='application/json')

@pattern_bp.route('/patternslice/new')
def pattern_slice__new() -> Response:
    patternSlice = PatternSlice()
    return Response(patternSlice.to_json(), mimetype='application/json')

@pattern_bp.route('/patternblock/new')
def pattern_block_new() -> Response:
    patternBlock = PatternBlock("New Pattern")
    return Response(patternBlock.to_json(), mimetype='application/json')

@pattern_bp.route('/pattern/load/<name>')
def pattern_object(name: str) -> Response:
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    return Response(pattern.to_json(), mimetype='application/json')

@pattern_bp.route('/pattern/save', methods=['POST'])
def save_pattern() -> Response:
    data = request.json
    if not data:
        return Response("Invalid data", status=400)
    
    pattern = Pattern.from_json(data)
    pattern.save_to_file(f'scratch/{pattern.identifier}.json')
    return Response("Pattern saved successfully", status=200)

@pattern_bp.route('/svg/pattern/<name>')
def pattern_svg(name: str) -> Response:
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    svgType = request.args.get('type', 'full')
    latestWrittenSlice = int(request.args.get('lw', 0))
    showText = request.args.get('text', 'false').lower() == 'true'

    svgContent = generate_svg_content(evaluator, svgType, latestWrittenSlice, showText=showText)
    return Response(svgContent, mimetype='image/svg+xml')

def generate_svg_content(evaluator: PatternEvaluator, svgType: str, latestWrittenSlice: int, showText: bool = True) -> str:
    if svgType == 'written':
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, preColour='lightblue', colour='white', showText=showText)
    elif svgType == 'unwritten':
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, showText=showText)
    elif svgType == 'lic':
        timePoint = evaluator.get_earliest_end_point_of_slice(latestWrittenSlice)
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, dayCut=timePoint, preColour='lightblue', colour='white', condition="and", showText=showText)
    elif svgType == 'lrc':
        timePoint = evaluator.get_earliest_end_point_of_slice(latestWrittenSlice)
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, dayCut=timePoint, preColour='white', colour='lightblue', condition="and", showText=showText)
    elif svgType == 'upr':
        timePoint = evaluator.get_earliest_end_point_of_slice(latestWrittenSlice)
        return evaluator.create_svg(evaluator.patternBlocks, dayCut=timePoint, showText=showText)
    else:
        return evaluator.create_svg(evaluator.patternBlocks, showText=showText)
