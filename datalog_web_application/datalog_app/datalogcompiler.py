from raco.datalog.grammar import parse
from raco.scheme import Scheme
from raco.catalog import ASCIIFile
from raco.language import PythonAlgebra, PseudoCodeAlgebra, CCAlgebra, ProtobufAlgebra
from raco.algebra import LogicalAlgebra
from raco.compile import compile, optimize, common_subexpression_elimination

import webapp2

page = """
<html>
<head>
<title>Datalog Compiler</title>


<link href="stylesheets/layout.css" type="text/css" rel="stylesheet" />
<script type="text/javascript" src="javascripts/jquery.js"></script>
<script type="text/javascript" src="http://d3js.org/d3.v2.js"></script>

<script type="text/javascript">

// the last language that was selected
var target_language = "CCAlgebra";

function handleerrors(request, display) {
  request.success(function(result) {
    $(display).text(result);
  });

  request.error(function(jqXHR, textStatus, errorThrown) {
    if (textStatus == 'timeout')
      $(display).text("Server is not responding");
 
    if (textStatus == 'error')
      var msg = '<div class="error"><a href="';
      msg = msg + this.url;
      msg = msg + '">Error</a></div>';
      $(display).html(msg);
  });
}

function getplan() {
  var query = $("#query").val();
  var request = $.get("plan", {query:query});
  handleerrors(request, "#plan");
};

function optimizeplan() {
  getplan(); // make sure the plan matches the query
  var query = $("#query").val();
  target_language = $(this).val(); //global variable
  var request = $.get("optimize", {query:query, target:target_language});
  handleerrors(request, "#optimized");
}

function compileplan() {
  var query = $("#query").val();
  var url = "compile?" + $.param({query:query, target:target_language});
  document.location.href=url;
}

$(document).ready(function(){
  $("#query").bind('keyup change', function() {
  $(".display").empty();
});
  $(".planner").click(getplan);
  $(".optimizer").click(optimizeplan);
  $(".compiler").click(compileplan);
  $(".example").click(function(){
$(".display").empty();
var example_query = $(this).text();
$("#query").val(example_query);
getplan();
});
  $(".display").css("border-style", "solid");
  $(".error").css("font-color", "red");
  $(".label").css("font-size", "small");
  $(".label").css("font-style", "italic");
  $(".display").css("width", 800);
  $(".display").css("height", 200);
});
</script>


</head>
<body>
  <div class="content">
    <div class="top_block header">
      <div class="content" class="title">
<h2>Datalog Rule Compiler</h2> 
      </div>
    </div>
    <div class="background region">
      <div class="background menu">
      </div>
    </div>
        <div class="left_block menu">
          <div class="content">
<div class="examples">
<div><em>Examples (click)</em></div>
<div class="label">Select</div>
<div class="example">A(x) :- R(x,3)</div>
<div class="label">Select2</div>
<div class="example">A(x) :- R(x,y),S(y,z,4),z<3</div>
<div class="label">Self-join</div>
<div class="example">A(x,z) :- R(x,y),R(y,z)</div>
<div class="label">Triangle</div>
<div class="example">A(x,z) :- R(x,y),S(y,z),T(z,x)</div>
<div class="label">Tournament</div>
<div class="example">A(A,B,C) :- R(A,x,B),R(A,y,C),R(C,z,B)</div>
<div class="label">Cross Product</div>
<div class="example">A(x,z) :- R(x,y),S(y),T(z)</div>
<div class="label">Two cycles</div>
<div class="example">A(x,z) :- R(x,y),S(y,a,z),T(z,b,x),W(a,b)</div>
<div class="label">Two Rules</div>
<div class="example">A(x,z) :- R(x,y,z);
B(w) :- A(3,w)</div>

</div>
          </div>
        </div>
    <div class="center_block region">
      <div class="content">
<textarea cols="50" rows="2" id="query">
%(query)s
</textarea>
<div/>
<input class="planner" type="submit" value="to RA">
<div class="display" id="plan">
%(expression)s
</div>
<input class="optimizer" type="submit" value="ProtobufAlgebra">
<input class="optimizer" type="submit" value="CCAlgebra">
<input class="optimizer" type="submit" value="PythonAlgebra">
<input class="optimizer" type="submit" value="PseudoCodeAlgebra">
<div class="display" id="optimized">
</div>
<input class="compiler" type="submit" value="Compile">
<div id="#response"/>
<div id="sig"/>
      </div>
    </div>
  </div>
</body>
</html>
"""

defaultquery = """A(x,z) :- R(x,y),S(y,z),T(z,x)"""

def programplan(query):
  # parse it
  parsedprogram = parse(query)
 
  # generate an RA expression
  exprs = parsedprogram.toRA()

  return exprs

def format(expressions):
  return "\n".join(["%s" % e for e in expressions])

class MainPage(webapp2.RequestHandler):
  def get(self,query=defaultquery):
  
    expressions = programplan(query)

    expression = format(expressions)

    self.response.headers['Content-Type'] = 'text/html'
    self.response.write(page % locals())
    
class Plan(webapp2.RequestHandler):
  def get(self):
    query = self.request.get("query")
    expressions = programplan(query)

    plan = format(expressions)
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write(plan)

class Optimize(webapp2.RequestHandler):
  def get(self):
    query = self.request.get("query")
    target = self.request.get("target")
    expressions = programplan(query)
    targetalgebra = globals()[target] # assume the argument is in local scope
    def opt(plan):
      return optimize(expr, target=targetalgebra, source=LogicalAlgebra)

    optimized = format([opt(expr) for expr in expressions])

    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write(optimized)

class Compile(webapp2.RequestHandler):
  def get(self):
    query = self.request.get("query")
    target = self.request.get("target")
    expressions = programplan(query)
    targetalgebra = globals()[target] # assume the argument is in local scope
    def opt(plan):
      optimized = optimize(expr, target=targetalgebra, source=LogicalAlgebra)
      optimized = common_subexpression_elimination(optimized)
      compiled = compile(optimized)
      return optimized

    optimized = [opt(expr) for expr in expressions]

    # We can't compile a set of rules yet. Just grab the first one.
    compiled = compile(optimized[0])

    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write(compiled)

app = webapp2.WSGIApplication([
   ('/', MainPage),
   ('/plan',Plan),
   ('/optimize',Optimize),
   ('/compile',Compile)
  ],
  debug=True
)

"""
TODO: 
Debug conditions: A(x,z) :- R(x,p1,y),R(y,p2,z),R(z,p3,w)
Multiple rules
Recursion
Show graph visually
Protobuf
Show parse errors (with link to error)
"""
