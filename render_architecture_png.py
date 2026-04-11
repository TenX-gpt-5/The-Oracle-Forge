from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


OUT = Path("/Users/gersumasfaw/week8_9/oracle_forge_architecture_v2.png")

W = 1800
H = 2300
BG = "#0B0D12"
TEXT = "#F6E7C5"
SUBTEXT = "#D8C69B"
LINE = "#F1A501"
BOX_OUTLINE = "#F1A501"

TOP = "#4F46B5"
KB = "#6F3F09"
CONTEXT = "#7C4A0F"
EXEC = "#5146B8"
TRANSFORM = "#0E5B57"
VALIDATE = "#8A3B19"
EVAL = "#0C6B58"


def load_font(size: int, bold: bool = False):
    candidates = []
    if bold:
        candidates += [
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/Library/Fonts/Arial Bold.ttf",
            "/System/Library/Fonts/SFNS.ttf",
        ]
    candidates += [
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/SFNS.ttf",
    ]
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


FONT_H1 = load_font(54, bold=True)
FONT_H2 = load_font(44, bold=True)
FONT_H3 = load_font(36, bold=True)
FONT_BODY = load_font(28, bold=False)


def text_size(draw: ImageDraw.ImageDraw, text: str, font):
    bbox = draw.multiline_textbbox((0, 0), text, font=font, spacing=6, align="center")
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def centered_text(draw, box, title, subtitle=None, title_font=FONT_H2, sub_font=FONT_BODY):
    x1, y1, x2, y2 = box
    cx = (x1 + x2) / 2
    if subtitle:
        tw, th = text_size(draw, title, title_font)
        sw, sh = text_size(draw, subtitle, sub_font)
        total_h = th + 14 + sh
        ty = (y1 + y2 - total_h) / 2
        draw.multiline_text((cx, ty), title, fill=TEXT, font=title_font, anchor="ma", align="center", spacing=6)
        draw.multiline_text((cx, ty + th + 14), subtitle, fill=SUBTEXT, font=sub_font, anchor="ma", align="center", spacing=6)
    else:
        tw, th = text_size(draw, title, title_font)
        ty = (y1 + y2 - th) / 2
        draw.multiline_text((cx, ty), title, fill=TEXT, font=title_font, anchor="ma", align="center", spacing=6)


def rounded_box(draw, box, fill, radius=28, outline=BOX_OUTLINE, width=6):
    x1, y1, x2, y2 = box
    shadow = (x1 + 8, y1 + 10, x2 + 8, y2 + 10)
    draw.rounded_rectangle(shadow, radius=radius, fill="#000000")
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def arrow(draw, start, end, color=LINE, width=6, head=18):
    x1, y1 = start
    x2, y2 = end
    draw.line((x1, y1, x2, y2), fill=color, width=width)
    if abs(y2 - y1) >= abs(x2 - x1):
        direction = 1 if y2 > y1 else -1
        draw.polygon(
            [
                (x2, y2),
                (x2 - head, y2 - direction * head),
                (x2 + head, y2 - direction * head),
            ],
            fill=color,
        )
    else:
        direction = 1 if x2 > x1 else -1
        draw.polygon(
            [
                (x2, y2),
                (x2 - direction * head, y2 - head),
                (x2 - direction * head, y2 + head),
            ],
            fill=color,
        )


def main():
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    title = "Oracle Forge Architecture V2"
    subtitle = "Planner-driven, context-rich, validated, and self-correcting"
    draw.text((W / 2, 58), title, fill="#F8F3E8", font=FONT_H1, anchor="ma")
    draw.text((W / 2, 116), subtitle, fill="#C7CCD6", font=FONT_BODY, anchor="ma")

    top_box = (360, 170, 1440, 320)
    rounded_box(draw, top_box, TOP)
    centered_text(draw, top_box, "Oracle Forge Agent", "Receives question and manages the turn")

    planner_box = (170, 390, 860, 530)
    context_box = (940, 390, 1630, 530)
    rounded_box(draw, planner_box, TOP)
    rounded_box(draw, context_box, TOP)
    centered_text(draw, planner_box, "Planner", "Intent, entities, required DBs,\noutput shape")
    centered_text(draw, context_box, "Context Service", "Retrieves only the relevant\ncontext layers")

    kb_box = (90, 610, 1710, 1020)
    rounded_box(draw, kb_box, KB, radius=32)
    centered_text(draw, (90, 630, 1710, 720), "Knowledge Base and Retrieval Context", None, title_font=FONT_H2)

    small_boxes = [
        ((130, 760, 490, 920), CONTEXT, "KB-v1", "Architecture rules\nand agent policies"),
        ((530, 760, 890, 920), CONTEXT, "KB-v2", "Domain definitions\nand business semantics"),
        ((930, 760, 1290, 920), CONTEXT, "KB-v3", "Corrections log\nand successful fixes"),
        ((1330, 760, 1690, 920), CONTEXT, "Schema / Usage Index", "Tables, columns, keys,\nsample joins"),
    ]
    for box, color, title_text, subtitle_text in small_boxes:
        rounded_box(draw, box, color, radius=24, width=4)
        centered_text(draw, box, title_text, subtitle_text, title_font=FONT_H3, sub_font=FONT_BODY)

    join_box = (330, 940, 790, 1000)
    text_box = (1010, 940, 1470, 1000)
    rounded_box(draw, join_box, CONTEXT, radius=20, width=4)
    rounded_box(draw, text_box, CONTEXT, radius=20, width=4)
    centered_text(draw, join_box, "Join-key intelligence", None, title_font=FONT_BODY)
    centered_text(draw, text_box, "Text-field inventory", None, title_font=FONT_BODY)

    exec_box = (300, 1110, 1500, 1260)
    rounded_box(draw, exec_box, EXEC)
    centered_text(draw, exec_box, "Execution Manager", "Builds subqueries and coordinates tools")

    router_box = (140, 1340, 1060, 1480)
    transform_box = (1120, 1340, 1660, 1480)
    rounded_box(draw, router_box, EXEC)
    rounded_box(draw, transform_box, TRANSFORM)
    centered_text(draw, router_box, "MCP Toolbox Router", "Dispatches to the correct DB tool")
    centered_text(draw, transform_box, "Transform + Extraction Layer", "Cross-DB joins, key normalization,\ntext extraction")

    db_boxes = [
        ((140, 1545, 360, 1655), EXEC, "PostgreSQL"),
        ((390, 1545, 610, 1655), EXEC, "SQLite"),
        ((640, 1545, 860, 1655), EXEC, "DuckDB"),
        ((890, 1545, 1110, 1655), EXEC, "MongoDB"),
    ]
    for box, color, label in db_boxes:
        rounded_box(draw, box, color, radius=22, width=4)
        centered_text(draw, box, label, None, title_font=FONT_BODY)

    validator_box = (360, 1755, 1440, 1915)
    rounded_box(draw, validator_box, VALIDATE)
    centered_text(draw, validator_box, "Validator", "Checks execution success, joins, row counts,\nanswer shape, and evidence grounding")

    self_correct_box = (180, 1995, 860, 2145)
    synth_box = (940, 1995, 1620, 2145)
    rounded_box(draw, self_correct_box, VALIDATE)
    rounded_box(draw, synth_box, EVAL)
    centered_text(draw, self_correct_box, "Self-Correction Loop", "Detect error -> diagnose -> fetch context -> retry")
    centered_text(draw, synth_box, "Answer + Evaluation", "Final answer, trace, score, regression log,\nreview gate into KB-v3")

    # Main vertical arrows
    arrow(draw, ((top_box[0] + top_box[2]) / 2, top_box[3]), ((top_box[0] + top_box[2]) / 2, planner_box[1]))
    arrow(draw, ((planner_box[0] + planner_box[2]) / 2, planner_box[3]), ((exec_box[0] + exec_box[2]) / 2 - 250, kb_box[1]))
    arrow(draw, ((context_box[0] + context_box[2]) / 2, context_box[3]), ((exec_box[0] + exec_box[2]) / 2 + 250, kb_box[1]))

    # Arrow from KB to execution manager
    arrow(draw, ((kb_box[0] + kb_box[2]) / 2, kb_box[3]), ((exec_box[0] + exec_box[2]) / 2, exec_box[1]))

    # execution to router/transform
    arrow(draw, (900, exec_box[3]), (600, router_box[1]))
    arrow(draw, (1000, exec_box[3]), (1390, transform_box[1]))

    # router to DB boxes
    router_mid = (router_box[0] + router_box[2]) / 2
    for box, _, _ in db_boxes:
        cx = (box[0] + box[2]) / 2
        draw.line((router_mid, router_box[3], cx, box[1]), fill=LINE, width=6)
        arrow(draw, (cx, box[1]), (cx, box[1] + 2))

    # dbs and transform to validator
    for box, _, _ in db_boxes:
        cx = (box[0] + box[2]) / 2
        draw.line((cx, box[3], 900, validator_box[1]), fill=LINE, width=5)
    draw.line(((transform_box[0] + transform_box[2]) / 2, transform_box[3], 980, validator_box[1]), fill=LINE, width=5)
    arrow(draw, (900, 1725), (900, validator_box[1]))

    # validator to self-correct and synth/eval
    arrow(draw, (760, validator_box[3]), (520, self_correct_box[1]))
    arrow(draw, (1040, validator_box[3]), (1280, synth_box[1]))

    # self-correction looping back
    draw.line((180, 2070, 90, 2070, 90, 1185, 300, 1185), fill=LINE, width=5)
    arrow(draw, (300, 1185), (302, 1185))
    draw.line((180, 2110, 60, 2110, 60, 470, 940, 470), fill="#A0A7B7", width=4)
    arrow(draw, (940, 470), (942, 470), color="#A0A7B7", width=4, head=14)

    # small caption
    caption = "Benchmark-winning focus: explicit planning, context retrieval, transformation, validation, and controlled learning."
    draw.text((W / 2, 2248), caption, fill="#C7CCD6", font=FONT_BODY, anchor="ma")

    img.save(OUT)


if __name__ == "__main__":
    main()
