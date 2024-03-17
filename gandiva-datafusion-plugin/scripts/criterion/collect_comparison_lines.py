import os

html_template = '''
<!DOCTYPE html>
<html>

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <title>select t_boolean and t_boolean2 from tab Summary - Criterion.rs</title>
    <style type="text/css">
        body {
            font: 14px Helvetica Neue;
            text-rendering: optimizelegibility;
        }

        .body {
            width: 960px;
            margin: auto;
        }

        a:link {
            color: #1F78B4;
            text-decoration: none;
        }

        h2 {
            font-size: 36px;
            font-weight: 300;
        }

        h3 {
            font-size: 24px;
            font-weight: 300;
        }

        #footer {
            height: 40px;
            background: #888;
            color: white;
            font-size: larger;
            font-weight: 300;
        }

        #footer a {
            color: white;
            text-decoration: underline;
        }

        #footer p {
            text-align: center
        }
    </style>
</head>

<body>
    <div class="body">
IMAGES
    </div>
</body>

</html>
'''

def create_html(criterion_dir):
    lines = []
    for entry in os.listdir(criterion_dir):
        if entry != "report":
            lines.append(entry)
    
    lines.sort()
    images = ''
    for entry in lines:
        image = '        <img src="' + os.path.join(criterion_dir, entry) + '/report/lines.svg"/>'
        if images != '':
            images += "\n"
        images += image

    html = html_template.replace('IMAGES', images)
    with open('index.html', 'w') as file:
        file.write(html)

create_html('../../../target/criterion')
