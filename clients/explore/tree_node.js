var React = require('react');
var Immutable = require('immutable');

var style = {
  outer: {
    fontFamily: "Consolas, monospace"
  },
  collapsed: {
    display: 'inline-block',
    transform: 'rotate(-90deg)',
    WebkitTransform: 'rotate(-90deg)'
  },
  expanded: {
    display: 'inline-block',
  },
  inner: {
    marginLeft: "20px",
  },
  types: {
    collection: { color: "#b0b0b0" },
    string: { color: "#798953" },
    int: { color: "#4562d2" },
    float: { color: "#d28445" },
    boolean: { color: "#75b5aa" }
  },
  contextSpan: {
    color: "#AAA"
  }
};

var isInteger = Number.isInteger || function(nVal) {
    return typeof nVal === "number" && isFinite(nVal) && nVal > -9007199254740992 && nVal < 9007199254740992 && Math.floor(nVal) === nVal;
}

var TreeNode = React.createClass({
  getInitialState: function() {
    return {
      expand: this.props.expandAll,
      expandAll: this.props.expandAll
    };
  },

  isCollection: function(value) {
    return value instanceof Immutable.List ||
           value instanceof Immutable.Set ||
           value instanceof Immutable.Map;
  },

  getTypeOf: function(value) {
    if (this.isCollection(value))
      return "collection";
    var type = typeof value;
    if (type == "number") {
      return isInteger(value) ? "int" : "float"
    }
    return type;
  },

  getCollectionName: function(value) {
    if (value instanceof Immutable.List)
      return "List";
    if (value instanceof Immutable.Set)
      return "Set";
    if (value instanceof Immutable.Map)
      return "Map";
  },

  toString: function(value) {
    if (this.isCollection(value)) {
      return this.getCollectionName(value) + " (" + value.size + " values)";
    }

    return String(value);
  },

  toggleExpand: function(e) {
    this.setState({
      expand: !this.state.expand,
      expandAll: e.getModifierState("Shift")
    });
  },

  render: function() {
    var value = this.props.value;
    var type = this.getTypeOf(value);
    var isCollection = this.isCollection(value);

    var arrowStyle = this.state.expand ? 'expanded' : 'collapsed';
    var bulletDiv = isCollection ?
      React.DOM.div({ style: style[arrowStyle] }, '\u25BE') :
      React.DOM.span({}, ' ');

    var headerItems = [ bulletDiv, React.DOM.span({}, '  ') ];

    if (this.props.name != undefined) {
      headerItems.push(React.DOM.span({}, this.props.name + ": "))
    }
    headerItems.push(React.DOM.span({ style: style.types[type] }, this.toString(value)))
    var header = React.DOM.div({ style: style.header, onClick: this.toggleExpand }, headerItems);

    var content = [ header ];
    if (this.state.expand && isCollection) {
      var isSet = value instanceof Immutable.Set;
      value.forEach(function(subvalue, index) {
        var name = isSet ? undefined : index;
        content.push(TreeNodeFactory({ value: subvalue, name: name, expandAll: this.state.expandAll }));
      }, this);
    }

    return React.DOM.div({ style: style.outer },
      React.DOM.div({ style: style.inner }, content)
    );
  }
});

var TreeNodeFactory = React.createFactory(TreeNode);

module.exports = TreeNode;
