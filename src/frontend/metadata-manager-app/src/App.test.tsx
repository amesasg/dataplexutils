import React from 'react';
import { render } from '@testing-library/react';
import '@testing-library/jest-dom';
import App from './App';

describe('App', () => {
  test('renders Metadata Manager heading', () => {
    const { getByText } = render(<App />);
    const headingElement = getByText(/Metadata Manager/i);
    expect(headingElement).toBeInTheDocument();
  });
});
