const globals = require('globals');

module.exports = {
  ignores: [
    'config/env/**'
  ],
  languageOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
    globals: {
      ...globals.commonjs,
      ...globals.es6,
      ...globals.node,
      expect: 'readonly',
      describe: 'readonly',
      it: 'readonly'
    }
  },
  rules: {
    'no-const-assign': 'error', // Ensure this rule is enabled
    'space-before-function-paren': 'off',
    'no-extra-semi': 'off',
    'object-curly-spacing': ['error', 'always'],
    'brace-style': ['error', 'stroustrup', { allowSingleLine: true }],
    'no-useless-escape': 'off',
    'standard/no-callback-literal': 'off',
    'new-cap': 'off',
    'no-console': ['warn', { allow: ['warn', 'error'] }]
  }
};