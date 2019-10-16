var tipRulesOut = '';
function draw_rule_output(d, hideempty, itMin, itMax, minDer, maxDer) {
    var data;
    if (hideempty == true || itMin > 0 || itMax != -1 || minDer > 0 || maxDer != -1) {
        var newdata = [];
        for(var i = 0; i < d.length; ++i) {
            var ok = true;
            if (hideempty && d[i].der == 0) {
                ok = false;
            }

            if (ok && d[i].it < itMin) {
                ok = false;
            }

            if (ok && itMax != -1 && d[i].it > itMax) {
                ok = false;
            }

            if (ok && d[i].der < minDer) {
                ok = false;
            }

            if (ok && maxDer != -1 && d[i].der > maxDer) {
                ok = false;
            }

            if (ok == true) {
                newdata.push({it: d[i].it, der: d[i].der, rule: d[i].rule, timeexec: d[i].timeexec});
            }
        }
        data = newdata;
    } else {
        data = d;
    }

// var barWidth = 10
var barWidth = d3.max([2, (10 - (data.length / 400))]);
var width = d3.max([50, data.length * (barWidth + 1)]);
var height = 300;
var margin = {top: 10, bottom: 30, left: 30, right: 0 };

var x = d3.scale.linear().domain([0, data.length]).range([0, width]);
var y = d3.scale.linear().domain([0, d3.max(data, function(datum) { return datum.der; })]).
  rangeRound([0, height]);

if (tipRulesOut != '') {
    tipRulesOut.hide();
}

tipRulesOut = d3.tip()
  .attr('class', 'd3-tip')
  .offset([30, 90])
  .html(function(d) {
      //Highlight the column
      return "<div><strong>Iteration: </strong>" + d.it + "<strong><br/>N.Derivations: </strong>" + Number(d.der).toLocaleString('en') + "<br/><strong>Rule: </strong>" + allrules[d.rule] + "<br/><strong>Runtime: </strong>" + d.timeexec + " ms.</div>";
  });

// add the canvas to the DOM
document.getElementById("ruleoutput").innerHTML = '';
var barchart = d3.select("#ruleoutput").
  append("svg:svg").
  attr("width", width + margin.left + margin.right).
  attr("height", height + margin.top + margin.bottom); 

barchart.call(tipRulesOut);

barchart.selectAll("rect").
  data(data).
  enter().
  append("svg:rect").
  attr("x", function(datum, index) { return margin.left + index * (barWidth + 1); }).
  attr("y", function(datum) { return margin.top + height - y(datum.der); }).
  attr("height", function(datum) { return y(datum.der); }).
  attr("width", barWidth).
  attr("fill", "#2d578b").
  attr("class", "bar").
  on("mouseover", tipRulesOut.show).
  on("mouseout", tipRulesOut.hide);          

barchart.append("text")      // text label for the x axis
        .attr("x", margin.left + width / 2)
        .attr("y", height + margin.top + 15)
        .style("text-anchor", "middle")
        .attr("style", "font-size: 14; font-family: Helvetica, sans-serif")
        .text("Iterations");

barchart.append("text")      // text label for the y axis
        .attr("x", -(height / 2))
        .attr("y", margin.top + 10)
        .attr("style", "font-size: 14; font-family: Helvetica, sans-serif")
        .attr("transform", "rotate(-90)")
        .text("N. Derivations");

var xAxis = d3.svg.axis().scale(x).ticks(0).orient("bottom");
barchart.append("g")
    .attr("class", "axis")
    .attr("transform", "translate(" + margin.left + "," + (height + margin.top) + ")")
    .call(xAxis);

var yAxis = d3.svg.axis().scale(y).orient("left").ticks(0);
barchart.append("g")
    .attr("class", "yaxis")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .call(yAxis);
}

var tipRulesOut1 = '';
function draw_rule_execs(d, itMin, itMax, minTime) {
    var data;
    if (itMin > 0 || itMax != -1 || minTime > 0) {
        var newdata = [];
        for(var i = 0; i < d.length; ++i) {
            var ok = true;

            if (ok && d[i].it < itMin) {
                ok = false;
            }

            if (ok && itMax != -1 && d[i].it > itMax) {
                ok = false;
            }

            if (ok && d[i].timeexec < minTime) {
                ok = false;
            }

            if (ok == true) {
                newdata.push({it: d[i].it, der: d[i].der, rule: d[i].rule, timeexec: d[i].timeexec});
            }
        }
        data = newdata;
    } else {
        data = d;
    }

    // var barWidth = 10
    var barWidth = d3.max([2, 10 - (data.length / 400)]);
    var width = d3.max([50, data.length * (barWidth + 1)]);
    var height = 300;
    var margin = {top: 10, bottom: 30, left: 30, right: 0 };

    var x = d3.scale.linear().domain([0, data.length]).range([0, width]);
    var y = d3.scale.linear().domain([0, d3.max(data, function(datum) { return datum.timeexec; })]).
      rangeRound([0, height]);

    if (tipRulesOut1 != '') {
        tipRulesOut1.hide();
    }

tipRulesOut1 = d3.tip()
  .attr('class', 'd3-tip')
  .offset([30, 90])
  .html(function(d) {
      return "<div><strong>Iteration: </strong>" + d.it + "<strong><br/>N.Derivations: </strong>" + Number(d.der).toLocaleString('en') + "<br/><strong>Rule: </strong>" + allrules[d.rule] + "<br/><strong>Runtime: </strong>" + d.timeexec + " ms.</div>";
  });

// add the canvas to the DOM
document.getElementById("ruleruntime").innerHTML = '';
var barchart = d3.select("#ruleruntime").
  append("svg:svg").
  attr("width", width + margin.left + margin.right).
  attr("height", height + margin.top + margin.bottom); 

barchart.call(tipRulesOut1);

barchart.selectAll("rect").
  data(data).
  enter().
  append("svg:rect").
  attr("x", function(datum, index) { return margin.left + index * (barWidth + 1); }).
  attr("y", function(datum) { return margin.top + height - y(datum.timeexec); }).
  attr("height", function(datum) { return y(datum.timeexec); }).
  attr("width", barWidth).
  attr("class", "bar").
  attr("fill", "#2d578b").
  on("mouseover", tipRulesOut1.show).
  on("mouseout", tipRulesOut1.hide);          

barchart.append("text")      // text label for the x axis
        .attr("x", margin.left + width / 2)
        .attr("y", height + margin.top + 15)
        .style("text-anchor", "middle")
        .attr("style", "font-size: 14; font-family: Helvetica, sans-serif")
        .text("Iterations");

barchart.append("text")      // text label for the y axis
        .attr("x", -(height / 2))
        .attr("y", margin.top + 10)
        .attr("style", "font-size: 14; font-family: Helvetica, sans-serif")
        .attr("transform", "rotate(-90)")
        .text("Runtime");

var xAxis = d3.svg.axis().scale(x).ticks(0).orient("bottom");
barchart.append("g")
    .attr("class", "axis")
    .attr("transform", "translate(" + margin.left + "," + (height + margin.top) + ")")
    .call(xAxis);

var yAxis = d3.svg.axis().scale(y).orient("left").ticks(0);
barchart.append("g")
    .attr("class", "yaxis")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .call(yAxis);
}
