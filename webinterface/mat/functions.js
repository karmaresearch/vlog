//Global vars for the graph
var ruleOutputs = [];
var allrules = [];
var port = "WEB_PORT";
var timeouts = {}
var intervals = {}

function getProgramInfo() {
    var info;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://" + window.location.hostname + ":" + port + "/getprograminfo", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            info = JSON.parse(http_request.responseText);
            content = "N. loaded rules: " + info.nrules + "";
            document.getElementById('detailsprogram').innerHTML = content;
            allrules = info.rules;
        }
    };
    http_request.send(null);
}

function msgbox(typ, container, msg, timeout) {
    var clazz = 'msgSuccess';
    if (typ != 'ok') {
        clazz = 'msgError';
    }
    cnt = '<p class="' + clazz + '">' + msg + '</p>';
    $(container).html(cnt);
    $(container).show();
    var id = setTimeout(function() { $(container).fadeOut(); }, timeout);
    timeouts[container] = id;
}

function getEDBInfo() {
    var info;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://" + window.location.hostname + ":" + port + "/getedbinfo", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            info = JSON.parse(http_request.responseText);
            content = "<table>";
            for(var i = 0; i < info.length; ++i) {
                content += "<tr><td>";
                content += "<b>Name:</b></td><td>" + info[i].name;
                content += "</td></tr>";
                content += "<tr><td>&nbsp;&nbsp;&nbsp;<em>Arity</em></td><td>" + info[i].arity + "</td></tr>"
                content += "<tr><td>&nbsp;&nbsp;&nbsp;<em>Size</em></td><td>" + info[i].size + "</td></tr>"
                content += "<tr><td>&nbsp;&nbsp;&nbsp;<em>Type</em></td><td>" + info[i].type + "</td></tr>"
            }
            content += "<\/table";
            document.getElementById('detailsedb').innerHTML = content;
        }
    };
    http_request.send(null);
}


function loadfile(el,dest) {
    var f = el.files;
    var reader = new FileReader();
    reader.onloadend = function(evt) {
        if (evt.target.readyState == FileReader.DONE) {
            document.getElementById(dest).value = reader.result;
        }
    }
    reader.readAsText(f[0]);
}

function launchMat() {
    //Populate the framebox with the content of newmat.html
    clearTimeout(timeouts["messageBox"]);
    $('#buttonMat').prop("disabled", true);
    //Clean the graphs
    ruleOutputs = [];
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://" + window.location.hostname + ":" + port + "/launchMat", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done) {
            var content = http_request.responseText;
            if (http_request.status === ok) {
                $('#frameBox').html(content);
                $('#frameBox').show();
                setRefresh(refreshMat);
            } else {
                msgbox('error',"#messageBox", content, 2000);
            }
            $('#buttonMat').prop("disabled", false);
        }
    };
    http_request.send(null);
}

function setupProgram() {
    var info;

    //Create the data for the form
    var contentform = '';
    var srule = document.getElementById('rulebox').value;
    contentform += 'rules=' + encodeURIComponent(srule);
    if (document.getElementById('automat').checked == true) {
        contentform += "&automat=on";
    } else {
        var spremat = document.getElementById('premat').value;
        contentform += '&queries=' + encodeURIComponent(spremat);
    }
    contentform = contentform.replace('/%20/g', '+');

    var http_request = new XMLHttpRequest();
    document.getElementById('buttonSetup').disabled = true;
    http_request.open("POST", "http://" + window.location.hostname + ":" + port + "/setup", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done) {
            if (http_request.status === ok) {
                //Refresh the program details
                getProgramInfo();
                msgbox('ok', '#messageBox','Program loaded!', 1500);
            }
            document.getElementById('buttonSetup').value='Load Rules';
            document.getElementById('buttonSetup').disabled = false;
        }
    };
    http_request.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    http_request.setRequestHeader('Content-Length', contentform.length);

    document.getElementById('buttonSetup').value='The program is loading ...';
    document.getElementById('buttonSetup').disabled = true;
    // And finally, We send our data.
    http_request.send(contentform);
}

function disablepremat() {
    var val = document.getElementById('premat').disabled;
    document.getElementById('premat').disabled = !val;
    document.getElementById('queryrule').disabled = !val;
}

function draw(ramperc) {
    var rp1 = radialProgress(document.getElementById('divRAM'))
        .label("")
        .diameter(150)
        .value(ramperc)
        .render();
}

function updateOutputGraph() {
    //Get all params
    var hide = document.getElementById('hideempty').checked;
    var startIt = document.getElementById('startIt').value;
    var stopIt = document.getElementById('stopIt').value;
    var minDer = document.getElementById('minDer').value;
    var maxDer = document.getElementById('maxDer').value;
    if (stopIt == 'inf') {
        stopIt = -1;
    }
    if (maxDer == 'inf') {
        maxDer = -1;
    }
    draw_rule_output(ruleOutputs, hide, +startIt, +stopIt, +minDer, +maxDer);
}

function updateRuntimeGraph() {
    var startIt = document.getElementById('startIt1').value;
    var stopIt = document.getElementById('stopIt1').value;
    if (stopIt == 'inf') {
        stopIt = -1;
    }
    var minTime = document.getElementById('minTime').value;
    draw_rule_execs(ruleOutputs, startIt, stopIt, minTime);
}

function refreshMem() {
    var stats;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://" + window.location.hostname + ":" + port + "/refreshmem", true);
    //http_request.timeout = 1000;
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            stats = JSON.parse(http_request.responseText);
            var ramperc = stats.ramperc;
            draw(ramperc);
            //Update the several fields
            document.getElementById('usedram').innerHTML = stats.usedmem;
        } else {
            if (http_request.status != ok) {
                if (refreshMem in intervals) {
                    clearInterval(intervals['refreshMem']);
                }
                document.getElementById('finished').innerHTML= 'Lost connection. No more refreshing.';
            }
        }
    };
    http_request.send(null);
}

function refreshMat() {
    //Get the various stats
    var stats;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://" + window.location.hostname + ":" + port + "/refresh", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            stats = JSON.parse(http_request.responseText);
            //Update the several fields
            document.getElementById('runtime').innerHTML = stats.runtime;
            var milli = +stats.runtime;
            var hours = parseInt(milli / (3600 * 1000));
            var sHours = hours.toString();
            if (hours < 10) {
                sHours = '0' + sHours;
            }
            milli -= hours * 3600 * 1000;
            var minutes = parseInt(milli / (60 * 1000));
            var sMinutes = minutes.toString();
            if (minutes < 10) {
                sMinutes = '0' + sMinutes;
            }
            milli -= minutes * 60 * 1000;
            var seconds = milli / 1000;
            var sSeconds = seconds.toFixed(3).toString();
            if (seconds < 10) {
                sSeconds = '0' + sSeconds;
            }

            document.getElementById('runtime').innerHTML = sHours + ":" + sMinutes + ":" + sSeconds;
            document.getElementById('iteration').innerHTML = stats.iteration;
            document.getElementById('rule').innerHTML = stats.rule;

            //Update the data for the rules output graph
            if (stats.outputrules != '') {
                var iterations = stats.outputrules.split(';');
                for(var i = 0; i < iterations.length; i++) {
                    var el = iterations[i].split(',');
                    ruleOutputs.push({it: +el[0], der: +el[1], rule: +el[2], timeexec: +el[3] });
                }
            }

            if (stats.finished == 'true') {
                get_size_IDBs();
                msgbox('ok', '#messageBox', 'Materialization is finished!', 5000);
                document.getElementById('rule').innerHTML = "Materialization is finished";
                if (refreshMat in intervals) {
                    clearInterval(intervals[refreshMat]);
                }
            }

            updateOutputGraph();
            updateRuntimeGraph();
        } else {
            if (http_request.status != ok) {
                document.getElementById('finished').innerHTML= 'Lost connection. No more refreshing.';
                clearInterval(interID);
            }
        }
    };
    http_request.send(null);
}

function showDetailIDBWindow(e, predicate) {
    var a = stats_IDB[predicate];
    var hc = '';
    for(var i = 0; i < a.length; ++i) {
        hc += 'Iteration: ' + a[i].it;
        hc += '<br/>Rule: ' + allrules[a[i].ruleid];
        hc += '<br/>Derivation: ' + Number(a[i].der).toLocaleString('en');
        hc += '<hr/>';
    }

    document.getElementById('detailsIDB').innerHTML = hc;
    //Move it to next the cursor
    var posx = 0;
    var posy = 0;
    if (!e) var e = window.event;
    if (e.pageX || e.pageY)
    {
        posx = e.pageX;
        posy = e.pageY;
    }
    else if (e.clientX || e.clientY)
    {
        posx = e.clientX;
        posy = e.clientY;
    }
    document.getElementById('detailsIDB').style.position = 'absolute';
    document.getElementById('detailsIDB').style.left = posx + 30;
    document.getElementById('detailsIDB').style.top = posy - 5;
    document.getElementById('detailsIDB').style.display = 'block';
}

function hideDetailIDBWindow() {
    document.getElementById('detailsIDB').style.display = 'none';
}

var stats_IDB = new Array();
function get_size_IDBs() {
    //Change the label of the button
    document.getElementById('buttonSizeIDBs').value = 'Calculating ...';
    var stats;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://localhost:" + port + "/sizeidbs", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            stats = JSON.parse(http_request.responseText);
            //Get size IDB predicates
            var allsizes = '<table><th>Predicate</th><th>#Rows</th>';
            var sIDBs = stats.sizeidbs.split(';');
            var totalsize = 0;
            for (var i = 0; i < sIDBs.length; i+=2) {
                //Parse the second string
                var detailsPredicate = sIDBs[i+1].split(',');
                var totalDer = 0;
                stats_IDB[sIDBs[i]] = [];
                for(var j = 0; j < detailsPredicate.length; j+=3) {
                    //0=iteration
                    //1=ruleid
                    //2=derivation
                    stats_IDB[sIDBs[i]].push({ it: +detailsPredicate[j], ruleid: +detailsPredicate[j+1], der: +detailsPredicate[j+2]});
                    totalDer += +detailsPredicate[j+2];
                }
                if (totalDer > 0) {
                    allsizes += '<tr><td><label onmouseout="hideDetailIDBWindow();" onmouseover="showDetailIDBWindow(event,\'' + sIDBs[i] + '\');">' + sIDBs[i] + '</label></td><td>' + Number(totalDer).toLocaleString('en')  + '</td></tr>';
                    totalsize += totalDer;
                }
            }
            allsizes += '</table>';
            allsizes = "<i>N. IDB facts: </i><label>" + Number(totalsize).toLocaleString('en') + "</label>" + allsizes;
            document.getElementById('sizeidbs').innerHTML = allsizes;
            document.getElementById('buttonSizeIDBs').value = 'Get size IDB tables';
        }
    };
    http_request.send(null);
}

function get_generic_stats() {
    var stats;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://localhost:" + port + "/genopts", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            stats = JSON.parse(http_request.responseText);
            var ramperc = stats.ramperc;
            draw(ramperc);
            //Update the several fields
            document.getElementById('totalram').innerHTML = stats.totmem;
            document.getElementById('commandline').innerHTML = stats.commandline;
            document.getElementById('nrules').innerHTML = stats.nrules;
            document.getElementById('nedbs').innerHTML = stats.nedbs;
            document.getElementById('nidbs').innerHTML = stats.nidbs;

            allrules = [];
            var r = stats.rules.split(";");
            for(var i = 0; i < r.length; ++i) {
                allrules.push(r[i]);
            }
        }
    };
    http_request.send(null);
}

function get_mem_commandline() {
    var stats;
    var http_request = new XMLHttpRequest();
    http_request.open("GET", "http://" + window.location.hostname + ":" + port + "/getmemcmd", true);
    http_request.onreadystatechange = function () {
        var done = 4, ok = 200;
        if (http_request.readyState === done && http_request.status === ok) {
            stats = JSON.parse(http_request.responseText);
            var ramperc = stats.ramperc;
            draw(ramperc);
            //Update the several fields
            document.getElementById('totalram').innerHTML = stats.totmem;
            document.getElementById('commandline').innerHTML = stats.commandline;
            //document.getElementById('tripleskb').innerHTML = new Intl.NumberFormat().format(Number(stats.tripleskb));
            //document.getElementById('termskb').innerHTML = new Intl.NumberFormat().format(Number(stats.termskb));
        }
    };
    http_request.send(null);
}

function setRefresh(inputfunct) {
    if (inputfunct in intervals) {
        //Clear the previous interval
        clearInterval(intervals[inputfunct]);
    }
    //Get the value in the box
    var refRate = document.getElementById('refreshRate').value;
    interID = setInterval(inputfunct, refRate);
    intervals[inputfunct] = interID;
}

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
