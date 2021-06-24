/** EasyWeb iframe v3.1.8 date:2020-05-04 License By http://ostudiogame.com */
layui.config({  // common.js是配置layui扩展模块的目录，每个页面都需要引入
    version: '318',   // 更新组件缓存，设为true不缓存，也可以设一个固定值
    base: getProjectUrl() + 'assets/module/'
}).extend({
    steps: 'steps/steps',
    notice: 'notice/notice',
    cascader: 'cascader/cascader',
    dropdown: 'dropdown/dropdown',
    fileChoose: 'fileChoose/fileChoose',
    Split: 'Split/Split',
    Cropper: 'Cropper/Cropper',
    tagsInput: 'tagsInput/tagsInput',
    citypicker: 'city-picker/city-picker',
    introJs: 'introJs/introJs',
    zTree: 'zTree/zTree'
}).use(['layer', 'admin'], function () {
    var $ = layui.jquery;
    var layer = layui.layer;
    var admin = layui.admin;

});

/** 获取当前项目的根路径，通过获取layui.js全路径截取assets之前的地址 */
function getProjectUrl() {
    var layuiDir = layui.cache.dir;
    if (!layuiDir) {
        var js = document.scripts, last = js.length - 1, src;
        for (var i = last; i > 0; i--) {
            if (js[i].readyState === 'interactive') {
                src = js[i].src;
                break;
            }
        }
        var jsPath = src || js[last].src;
        layuiDir = jsPath.substring(0, jsPath.lastIndexOf('/') + 1);
    }
    return layuiDir.substring(0, layuiDir.indexOf('assets'));
}

function getDate(timeStamp){
    var date = new Date(timeStamp);

    var year = date.getFullYear(); //获取完整的年份(4位)

    var month = date.getMonth() + 1; //获取当前月份(0-11,0代表1月)
    month = month<10?"0"+month:month

    var date = date .getDate(); //获取当前日(1-31)
    date = date<10?"0"+date:date

    var result = year + "-" + month + "-" + date;
    return result;
}

function getLastMonth(timeStamp){
    var date = new Date(timeStamp);

    var year = date.getFullYear(); //获取完整的年份(4位)

    var month = date.getMonth(); //获取当前月份(0-11,0代表1月)
    month = month<10?"0"+month:month

    var date = date .getDate(); //获取当前日(1-31)
    date = date<10?"0"+date:date

    var result = year + "-" + month;
    return result;
}

function getLastDate(timeStamp){
    var date = new Date(timeStamp);

    var year = date.getFullYear(); //获取完整的年份(4位)

    var month = date.getMonth() + 1; //获取当前月份(0-11,0代表1月)
    month = month<10?"0"+month:month

    var date = date .getDate(); //获取当前日(1-31)
    date = date<10?"0"+date:date

    var result = year + "-" + month + "-" + date;
    return result;
}

function getAMonthAgo(timeStamp){
    var date = new Date(timeStamp);

    var year = date.getFullYear(); //获取完整的年份(4位)

    var month = date.getMonth(); //获取当前月份(0-11,0代表1月)
    month = month<10?"0"+month:month

    var date = date .getDate(); //获取当前日(1-31)
    date = date<10?"0"+date:date

    var result = year + "-" + month + "-" + date;
    return result;
}

function getAYearAgo(timeStamp){
    var date = new Date(timeStamp);

    var year = date.getFullYear() - 1; //获取完整的年份(4位)

    var month = date.getMonth() + 1; //获取当前月份(0-11,0代表1月)
    month = month<10?"0"+month:month

    var date = date .getDate(); //获取当前日(1-31)
    date = date<10?"0"+date:date

    var result = year + "-" + month + "-" + date;
    return result;
}

function ajax(options) {
    options = options || {};
    options.type = (options.type || "GET").toUpperCase();
    options.dataType = options.dataType || "json";
    var params = formatParams(options.data);

    //创建 - 非IE6 - 第一步
    if (window.XMLHttpRequest) {
        var xhr = new XMLHttpRequest();
    } else { //IE6及其以下版本浏览器
        var xhr = new ActiveXObject('Microsoft.XMLHTTP');
    }

    //接收 - 第三步
    xhr.onreadystatechange = function () {
        if (xhr.readyState == 4) {
            var status = xhr.status;
            if (status >= 200 && status < 300) {
                options.success && options.success(xhr.responseText, xhr.responseXML);
            } else {
                options.fail && options.fail(status);
            }
        }
    }

    //连接 和 发送 - 第二步
    if (options.type == "GET") {
        xhr.open("GET", options.url + "?" + params, true);
        xhr.send(null);
    } else if (options.type == "POST") {
        xhr.open("POST", options.url, true);
        //设置表单提交时的内容类型
        xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
        xhr.send(params);
    }
}
//格式化参数
function formatParams(data) {
    var arr = [];
    for (var name in data) {
        arr.push(encodeURIComponent(name) + "=" + encodeURIComponent(data[name]));
    }
    arr.push(("v=" + Math.random()).replace(".",""));
    return arr.join("&");
}